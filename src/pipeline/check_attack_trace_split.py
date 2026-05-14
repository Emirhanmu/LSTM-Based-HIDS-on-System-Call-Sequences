from pathlib import Path


if __name__ == "__main__":
    attack_folder = Path("data/raw/php_cwe_434/test/normal_and_attack")
    attack_files = sorted([p.stem for p in attack_folder.glob("*.zip")])

    val_attack_max_files = 2

    val_attack = set(attack_files[:val_attack_max_files])
    test_attack = set(attack_files[val_attack_max_files:])

    overlap = val_attack & test_attack

    print("\n====================")
    print("ATTACK TRACE SPLIT CHECK")
    print("====================")
    print(f"Validation attack traces: {len(val_attack)}")
    print(f"Test attack traces:       {len(test_attack)}")
    print(f"Overlap:                  {len(overlap)}")

    print("\nValidation attack trace names:")
    print(sorted(val_attack))

    if overlap:
        print("\n❌ Overlap found:")
        print(sorted(overlap))
    else:
        print("\n✅ No overlap between validation-attack and test-attack trace names.")