# Upload availability report

- schedule (s): [0.0, 3.0, 8.0, 20.0, 45.0, 90.0]
- runs/size: 2, verifiers: 4, public_gw: True
- batch: 0x5f5b167c2f256b7e135fe8be0af65090ee236a2743965dd7439e7a3da8eed41d

## Fraction of (run x target) fully available by offset

| size | t=0.0s | t=3.0s | t=8.0s | t=20.0s | t=45.0s | t=90.0s | never |
|---|---|---|---|---|---|---|---|
| 1K | 100% | 100% | 100% | 100% | 100% | 100% | 0 |
| 1M | 100% | 100% | 100% | 100% | 100% | 100% | 0 |
| 3M | 100% | 100% | 100% | 100% | 100% | 100% | 0 |
| 4M | 100% | 100% | 100% | 100% | 100% | 100% | 0 |
| 8M | 100% | 100% | 100% | 100% | 100% | 100% | 0 |

## At-return (offset 0) availability per target

- **v1:2aa638**: 100% available at offset 0 (10/10)
- **v2:f1ebec**: 100% available at offset 0 (10/10)
- **v3:83eab7**: 100% available at offset 0 (10/10)
- **v4:a75fbe**: 100% available at offset 0 (10/10)
- **public-gw**: 100% available at offset 0 (10/10)

## Per-(size,run) detail

- 1K run0 ref=4a622be993 status=completed up=0.12s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 1K run1 ref=0c8a071281 status=completed up=0.11s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 1M run0 ref=f7ada71dfd status=completed up=3.37s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 1M run1 ref=4f145dfc12 status=completed up=3.07s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 3M run0 ref=eee00ef0ed status=completed up=6.42s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 3M run1 ref=d12182073a status=completed up=6.94s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 4M run0 ref=acfac942ce status=completed up=10.21s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 4M run1 ref=099100dbb6 status=completed up=6.53s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 8M run0 ref=f8d78f4759 status=completed up=13.85s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
- 8M run1 ref=f63011b22f status=completed up=14.74s first_file={'v1:2aa638': 0.0, 'v2:f1ebec': 0.0, 'v3:83eab7': 0.0, 'v4:a75fbe': 0.0, 'public-gw': 0.0}
