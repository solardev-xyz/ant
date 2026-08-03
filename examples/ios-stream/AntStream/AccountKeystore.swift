import Foundation
import Security

/// Where AntStream's account key lives.
///
/// Both existing example apps let the Rust library create and keep the
/// account key as plaintext `identity.json` inside the app container
/// (`crates/ant-ffi` → `load_or_create_identity`). That file is readable
/// by anything that can reach the container and rides along in device
/// backups. AntStream instead owns the key itself and hands it to the
/// library at startup through `ant_init_with_identity`, so the library
/// never writes key material to disk — the `KeyProvider` backend
/// PLAN.md § 5.10 plans for iOS.
///
/// ### What "Secure Enclave" can and cannot do here
///
/// The Secure Enclave only holds **P-256** keys; Swarm/Ethereum accounts
/// are **secp256k1**, so the account key itself can never be an enclave
/// key. What the enclave *can* do is hold a P-256 wrapping key that never
/// leaves the chip, and we encrypt the identity document to it (ECIES).
/// The Keychain then stores ciphertext that is useless on any other
/// device, even if the Keychain item is exfiltrated. That is the strongest
/// protection iOS offers for a secp256k1 secret, and it is what
/// ``Protection/secureEnclave`` means below.
///
/// Enclave keys are by definition device-bound, so enclave wrapping and
/// iCloud Keychain sync are mutually exclusive. The user picks:
///
/// * ``Protection/secureEnclave`` (default) — strongest at rest, but a
///   reinstall-on-a-new-device needs the exported account key to restore.
/// * ``Protection/iCloudKeychain`` — the item syncs, so a reinstall or a
///   new device silently recovers the same account.
/// * ``Protection/deviceOnly`` — plain Keychain item, used when the
///   enclave is unavailable (older simulators return `errSecUnimplemented`
///   for enclave key creation).
///
/// Every mode uses `kSecAttrAccessibleAfterFirstUnlock` so the node can
/// start from a `BGProcessingTask` after a reboot the user hasn't unlocked
/// past yet; the two device-bound modes add `…ThisDeviceOnly` so the item
/// is excluded from unencrypted backups.
enum AccountKeystore {
    /// How the stored identity is protected at rest.
    enum Protection: String, Codable, CaseIterable {
        /// Encrypted to a P-256 key that lives in the Secure Enclave.
        case secureEnclave = "secure-enclave"
        /// Plain Keychain item, this device only (enclave unavailable).
        case deviceOnly = "device-only"
        /// Plain Keychain item that syncs through iCloud Keychain.
        case iCloudKeychain = "icloud-keychain"

        var label: String {
            switch self {
            case .secureEnclave: return "Secure Enclave (this device)"
            case .deviceOnly: return "Keychain (this device)"
            case .iCloudKeychain: return "iCloud Keychain"
            }
        }

        /// Does a reinstall on another device recover the account by itself?
        var syncsToICloud: Bool { self == .iCloudKeychain }
    }

    enum KeystoreError: LocalizedError {
        case keychain(OSStatus)
        case enclave(String)
        case ffi(String)
        case corrupt(String)

        var errorDescription: String? {
            switch self {
            case .keychain(let status):
                let detail = SecCopyErrorMessageString(status, nil) as String?
                return "Keychain error \(status)\(detail.map { ": \($0)" } ?? "")"
            case .enclave(let m): return "Secure Enclave error: \(m)"
            case .ffi(let m): return m
            case .corrupt(let m): return "Stored account key is unreadable: \(m)"
            }
        }
    }

    // MARK: - Item coordinates

    private static let service = "at.vibing.ant.stream"
    private static let account = "account-identity"
    /// Application tag of the Secure Enclave wrapping key.
    private static let wrapKeyTag = Data("at.vibing.ant.stream.identity-wrap".utf8)
    private static let enclaveAlgorithm: SecKeyAlgorithm =
        .eciesEncryptionCofactorVariableIVX963SHA256AESGCM

    /// What actually goes in the Keychain: a small self-describing
    /// envelope so a read knows whether it must unwrap through the
    /// enclave, without a second lookup or a `UserDefaults` flag that
    /// could drift out of sync with the item.
    private struct Envelope: Codable {
        let version: Int
        let protection: Protection
        /// Base64 ciphertext for `.secureEnclave`; the identity JSON
        /// itself for the two plaintext modes.
        let payload: String
    }

    // MARK: - Public API

    /// The identity document to hand `ant_init_with_identity`, creating
    /// (and storing) one on first launch. Never writes the key to disk.
    static func loadOrCreateIdentity() throws -> String {
        if let existing = try loadIdentity() { return existing }
        let json = try generateIdentity()
        try store(identity: json, protection: preferredProtection())
        return json
    }

    /// The stored identity document, or `nil` when nothing is stored yet.
    static func loadIdentity() throws -> String? {
        guard let envelope = try readEnvelope() else { return nil }
        switch envelope.protection {
        case .deviceOnly, .iCloudKeychain:
            return envelope.payload
        case .secureEnclave:
            guard let ciphertext = Data(base64Encoded: envelope.payload) else {
                throw KeystoreError.corrupt("wrapped payload is not base64")
            }
            let plaintext = try enclaveDecrypt(ciphertext)
            guard let json = String(data: plaintext, encoding: .utf8) else {
                throw KeystoreError.corrupt("unwrapped payload is not UTF-8")
            }
            return json
        }
    }

    /// How the stored identity is currently protected; `nil` when there
    /// is nothing stored.
    static func currentProtection() -> Protection? {
        ((try? readEnvelope()) ?? nil)?.protection
    }

    /// Move the stored identity between iCloud-backed and device-bound
    /// protection. Reads the identity through the *old* protection first,
    /// so the key survives the switch either way.
    static func setICloudBackup(_ enabled: Bool) throws {
        guard let identity = try loadIdentity() else { return }
        let target: Protection = enabled ? .iCloudKeychain : preferredProtection()
        try store(identity: identity, protection: target)
    }

    /// Restore the account from a backed-up account key (64 hex chars,
    /// `0x` tolerated) and make it the stored identity. Returns the
    /// rebuilt identity document. Throws before touching the Keychain if
    /// the key is malformed, so a typo can't wipe a working account.
    @discardableResult
    static func restore(fromAccountKey key: String) throws -> String {
        let json = try identity(fromAccountKey: key)
        try store(identity: json, protection: currentProtection() ?? preferredProtection())
        return json
    }

    /// Adopt a plaintext `identity.json` the Rust library wrote in an
    /// earlier build (or that a user restored by hand): copy it into the
    /// Keychain and shred the file. Returns `true` when a file was
    /// migrated.
    ///
    /// The file is only deleted once its key is safe — either already
    /// superseded by a Keychain copy, or successfully stored. A failed
    /// store leaves the file alone rather than destroying the only copy
    /// of the account.
    @discardableResult
    static func migrateLegacyIdentityFile(at url: URL) -> Bool {
        let fm = FileManager.default
        guard fm.fileExists(atPath: url.path) else { return false }

        // The Keychain copy always wins; the stale file is then just a
        // liability sitting in the container.
        if ((try? loadIdentity()) ?? nil) != nil {
            try? fm.removeItem(at: url)
            return false
        }
        guard let data = try? Data(contentsOf: url),
              let json = String(data: data, encoding: .utf8),
              // Only adopt something that actually parses as an identity.
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              obj["signing_key"] is String
        else { return false }
        do {
            try store(identity: json, protection: preferredProtection())
        } catch {
            return false
        }
        try? fm.removeItem(at: url)
        return true
    }

    /// Forget the stored identity (both the Keychain item and the enclave
    /// wrapping key). Used by "Remove account from this device"; the
    /// account itself is only recoverable from the exported key
    /// afterwards.
    static func destroy() throws {
        try deleteItem(synchronizable: true)
        try deleteItem(synchronizable: false)
        deleteEnclaveKey()
    }

    // MARK: - FFI helpers

    /// A fresh identity from the library (`ant_identity_generate`).
    private static func generateIdentity() throws -> String {
        try ffiString("generate account key") { errPtr in ant_identity_generate(errPtr) }
    }

    /// The identity implied by an existing account key
    /// (`ant_identity_from_key`). Rejects malformed / out-of-range keys.
    static func identity(fromAccountKey key: String) throws -> String {
        let trimmed = key.trimmingCharacters(in: .whitespacesAndNewlines)
        return try ffiString("read account key") { errPtr in
            trimmed.withCString { ant_identity_from_key($0, errPtr) }
        }
    }

    private static func ffiString(
        _ what: String,
        _ body: (UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?) -> UnsafeMutablePointer<CChar>?
    ) throws -> String {
        var errPtr: UnsafeMutablePointer<CChar>? = nil
        guard let raw = body(&errPtr) else {
            let msg = errPtr.flatMap { String(cString: $0) } ?? "could not \(what)"
            if let errPtr { ant_free_string(errPtr) }
            throw KeystoreError.ffi(msg)
        }
        defer { ant_free_string(raw) }
        return String(cString: raw)
    }

    // MARK: - Keychain

    /// Enclave wrapping when the chip will give us a key, plain
    /// device-only storage otherwise (older simulators).
    private static func preferredProtection() -> Protection {
        (try? enclaveKey(createIfMissing: true)) != nil ? .secureEnclave : .deviceOnly
    }

    private static func store(identity json: String, protection: Protection) throws {
        let payload: String
        switch protection {
        case .deviceOnly, .iCloudKeychain:
            payload = json
        case .secureEnclave:
            payload = try enclaveEncrypt(Data(json.utf8)).base64EncodedString()
        }
        let envelope = Envelope(version: 1, protection: protection, payload: payload)
        let data = try JSONEncoder().encode(envelope)

        // Replace both the synced and the device-only variant: a
        // synchronizable item and a non-synchronizable one with the same
        // service/account are *different* Keychain items, and leaving the
        // old one behind would let a stale identity resurface after a
        // protection change.
        try deleteItem(synchronizable: true)
        try deleteItem(synchronizable: false)

        var attrs: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecValueData as String: data,
        ]
        if protection.syncsToICloud {
            attrs[kSecAttrSynchronizable as String] = kCFBooleanTrue
            // Synchronizable items cannot use a …ThisDeviceOnly class.
            attrs[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
        } else {
            attrs[kSecAttrSynchronizable as String] = kCFBooleanFalse
            attrs[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        }
        let status = SecItemAdd(attrs as CFDictionary, nil)
        guard status == errSecSuccess else { throw KeystoreError.keychain(status) }

        // The enclave wrapping key is dead weight once we've moved to a
        // plaintext (syncing) item; drop it so it can't linger.
        if protection != .secureEnclave { deleteEnclaveKey() }
    }

    private static func readEnvelope() throws -> Envelope? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            // A lookup that doesn't say "any" only ever sees the
            // non-synchronizable item, which would miss an iCloud-restored
            // account entirely.
            kSecAttrSynchronizable as String: kSecAttrSynchronizableAny,
            kSecReturnData as String: kCFBooleanTrue as Any,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var out: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &out)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess, let data = out as? Data else {
            throw KeystoreError.keychain(status)
        }
        do {
            return try JSONDecoder().decode(Envelope.self, from: data)
        } catch {
            throw KeystoreError.corrupt("\(error)")
        }
    }

    private static func deleteItem(synchronizable: Bool) throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecAttrSynchronizable as String: synchronizable ? kCFBooleanTrue as Any
                : kCFBooleanFalse as Any,
        ]
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeystoreError.keychain(status)
        }
    }

    // MARK: - Secure Enclave wrapping key

    /// The P-256 wrapping key held in the Secure Enclave, created on
    /// first use. Private-key usage only — no biometry gate, because the
    /// node has to start from a background task with no one watching.
    private static func enclaveKey(createIfMissing: Bool) throws -> SecKey {
        let query: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrKeyType as String: kSecAttrKeyTypeECSECPrimeRandom,
            kSecAttrApplicationTag as String: wrapKeyTag,
            kSecReturnRef as String: kCFBooleanTrue as Any,
        ]
        var out: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &out)
        if status == errSecSuccess, let found = out {
            // A key-class query with kSecReturnRef always yields a SecKey.
            guard CFGetTypeID(found) == SecKeyGetTypeID() else {
                throw KeystoreError.enclave("wrapping key had an unexpected type")
            }
            return found as! SecKey
        }
        guard status == errSecItemNotFound, createIfMissing else {
            throw KeystoreError.keychain(status)
        }

        var acError: Unmanaged<CFError>?
        guard let access = SecAccessControlCreateWithFlags(
            nil,
            kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            [.privateKeyUsage],
            &acError
        ) else {
            throw KeystoreError.enclave(cfErrorMessage(acError))
        }
        let attrs: [String: Any] = [
            kSecAttrKeyType as String: kSecAttrKeyTypeECSECPrimeRandom,
            kSecAttrKeySizeInBits as String: 256,
            kSecAttrTokenID as String: kSecAttrTokenIDSecureEnclave,
            kSecPrivateKeyAttrs as String: [
                kSecAttrIsPermanent as String: true,
                kSecAttrApplicationTag as String: wrapKeyTag,
                kSecAttrAccessControl as String: access,
            ],
        ]
        var genError: Unmanaged<CFError>?
        guard let key = SecKeyCreateRandomKey(attrs as CFDictionary, &genError) else {
            throw KeystoreError.enclave(cfErrorMessage(genError))
        }
        return key
    }

    private static func deleteEnclaveKey() {
        let query: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecAttrKeyType as String: kSecAttrKeyTypeECSECPrimeRandom,
            kSecAttrApplicationTag as String: wrapKeyTag,
        ]
        SecItemDelete(query as CFDictionary)
    }

    private static func enclaveEncrypt(_ plaintext: Data) throws -> Data {
        let priv = try enclaveKey(createIfMissing: true)
        guard let pub = SecKeyCopyPublicKey(priv) else {
            throw KeystoreError.enclave("wrapping key has no public half")
        }
        guard SecKeyIsAlgorithmSupported(pub, .encrypt, enclaveAlgorithm) else {
            throw KeystoreError.enclave("ECIES not supported by this key")
        }
        var error: Unmanaged<CFError>?
        guard let out = SecKeyCreateEncryptedData(
            pub, enclaveAlgorithm, plaintext as CFData, &error
        ) else {
            throw KeystoreError.enclave(cfErrorMessage(error))
        }
        return out as Data
    }

    private static func enclaveDecrypt(_ ciphertext: Data) throws -> Data {
        let priv = try enclaveKey(createIfMissing: false)
        var error: Unmanaged<CFError>?
        guard let out = SecKeyCreateDecryptedData(
            priv, enclaveAlgorithm, ciphertext as CFData, &error
        ) else {
            throw KeystoreError.enclave(cfErrorMessage(error))
        }
        return out as Data
    }

    private static func cfErrorMessage(_ error: Unmanaged<CFError>?) -> String {
        guard let error else { return "unknown" }
        return (error.takeRetainedValue() as Error).localizedDescription
    }
}
