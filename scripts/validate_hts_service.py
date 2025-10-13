"""
Validation script for HTS Context Service with real data
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.hts_context.service import HTSContextService
from src.services.hts_context.config import HTS_REFERENCE_PATH


def main():
    print("=" * 80)
    print("HTS CONTEXT SERVICE - REAL DATA VALIDATION")
    print("=" * 80)

    # Check if HTS file exists
    if not HTS_REFERENCE_PATH.exists():
        print(f"\nERROR: HTS reference file not found at: {HTS_REFERENCE_PATH}")
        print("Please ensure htsdata_ch73.json is in the data/input directory")
        return False

    print(f"\nHTS Reference File: {HTS_REFERENCE_PATH}")
    print(f"File Size: {HTS_REFERENCE_PATH.stat().st_size / 1024:.2f} KB")

    # Initialize service
    print("\nInitializing HTS Context Service...")
    try:
        service = HTSContextService()
        print("Service initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize service: {e}")
        return False

    # Get statistics
    print("\n" + "-" * 80)
    print("HTS REFERENCE STATISTICS")
    print("-" * 80)
    stats = service.get_hierarchy_statistics()

    print(f"\nTotal HTS Codes: {stats['total_codes']}")
    print(f"\nIndent Distribution:")
    for indent, count in sorted(stats["indent_distribution"].items()):
        print(f"  Indent {indent}: {count} codes")

    print(f"\nParent-Finding Statistics:")
    for stat_type, count in stats["parent_finding_stats"].items():
        print(f"  {stat_type}: {count}")

    if stats["orphaned_codes"]:
        print(f"\nOrphaned Codes Found: {len(stats['orphaned_codes'])}")
        print("First 5 orphaned codes:")
        for code in stats["orphaned_codes"][:5]:
            print(f"  - {code}")
    else:
        print("\nNo orphaned codes found")

    # Analyze parent-finding methods
    print("\n" + "-" * 80)
    print("PARENT-FINDING ANALYSIS")
    print("-" * 80)

    prefix_codes = []
    fallback_codes = []

    for code, node in service.hierarchy_map.items():
        parent = node["parent"]
        if parent:
            # Check if this was likely a prefix match or fallback
            if code.startswith(parent):
                prefix_codes.append(code)
            else:
                fallback_codes.append((code, parent, node["item"]["indent"]))

    print(f"\nPrefix Match Relationships: {len(prefix_codes)}")
    print(f"Fallback Relationships: {len(fallback_codes)}")

    if fallback_codes:
        print(f"\nSample Fallback Cases (first 5):")
        for code, parent, indent in fallback_codes[:5]:
            parent_indent = service.hierarchy_map[parent]["item"]["indent"]
            print(f"  {code} (indent={indent}) -> {parent} (indent={parent_indent})")

    # Test sample lookups
    print("\n" + "-" * 80)
    print("SAMPLE HTS CONTEXT LOOKUPS")
    print("-" * 80)

    # Select test codes from different indent levels
    test_codes = []

    # Get one code from each indent level
    for indent in sorted(stats["indent_distribution"].keys()):
        for code, node in service.hierarchy_map.items():
            if node["item"]["indent"] == indent:
                test_codes.append(code)
                break

    # Perform lookups
    for idx, test_code in enumerate(test_codes[:4], 1):
        print(f"\nTest {idx}: {test_code}")
        result = service.get_hts_context(test_code)

        if result["found"]:
            print(f"  Found: Yes")
            print(f"  Hierarchy Path ({len(result['hierarchy_path'])} levels):")
            for level in result["hierarchy_path"]:
                indent_spaces = "  " * level["indent"]
                print(
                    f"    {indent_spaces}[{level['indent']}] {level['code']}: {level['description'][:50]}..."
                )
        else:
            print(f"  Found: No")
            print(f"  Error: {result['error']}")

    # Validation checks
    print("\n" + "-" * 80)
    print("VALIDATION CHECKS")
    print("-" * 80)

    validation_passed = True

    # Check 1: All codes should be accessible
    print("\n1. Checking all codes are accessible...")
    inaccessible_count = 0
    for code in service.hierarchy_map.keys():
        if not service.validate_hts_code_exists(code):
            inaccessible_count += 1

    if inaccessible_count == 0:
        print("   PASS: All codes are accessible")
    else:
        print(f"   FAIL: {inaccessible_count} codes are not accessible")
        validation_passed = False

    # Check 2: Parent relationships should be valid
    print("\n2. Checking parent relationships...")
    invalid_parents = 0
    for code, node in service.hierarchy_map.items():
        parent = node["parent"]
        if parent and parent not in service.hierarchy_map:
            invalid_parents += 1
            print(f"   Invalid parent: {code} -> {parent}")

    if invalid_parents == 0:
        print("   PASS: All parent relationships are valid")
    else:
        print(f"   FAIL: {invalid_parents} invalid parent relationships")
        validation_passed = False

    # Check 3: Child relationships should be reciprocal
    print("\n3. Checking child-parent reciprocity...")
    reciprocity_errors = 0
    for code, node in service.hierarchy_map.items():
        for child_code in node["children"]:
            child_node = service.hierarchy_map.get(child_code)
            if not child_node or child_node["parent"] != code:
                reciprocity_errors += 1
                print(f"   Reciprocity error: {code} <-> {child_code}")

    if reciprocity_errors == 0:
        print("   PASS: All child-parent relationships are reciprocal")
    else:
        print(f"   FAIL: {reciprocity_errors} reciprocity errors")
        validation_passed = False

    # Check 4: Indent levels should be consistent with hierarchy
    print("\n4. Checking indent level relationships...")
    indent_warnings = 0
    severe_errors = 0

    for code, node in service.hierarchy_map.items():
        current_indent = node["item"]["indent"]
        parent = node["parent"]

        if parent:
            parent_indent = service.hierarchy_map[parent]["item"]["indent"]
            indent_diff = current_indent - parent_indent

            # In real HTS data, indent can skip levels
            # This is expected and handled by our fallback algorithm
            if indent_diff < 1:
                # Child should always have higher indent than parent
                severe_errors += 1
                if severe_errors <= 5:
                    print(
                        f"SEVERE: {code} (indent={current_indent}) -> parent {parent} (indent={parent_indent})"
                    )
            elif indent_diff > 3:
                # Large skips might indicate issues
                indent_warnings += 1
                if indent_warnings <= 3:
                    print(
                        f"WARNING: Large indent skip: {code} (indent={current_indent}) -> parent {parent} (indent={parent_indent}, diff={indent_diff})"
                    )

    if severe_errors == 0:
        print("   PASS: All indent relationships are valid")
        if indent_warnings > 0:
            print(
                f"   NOTE: {indent_warnings} large indent skips found (this is normal in HTS data)"
            )
    else:
        print(
            f"   FAIL: {severe_errors} severe indent errors (child indent <= parent indent)"
        )
        validation_passed = False

    # Export hierarchy map for debugging
    print("\n" + "-" * 80)
    print("EXPORTING DEBUG DATA")
    print("-" * 80)

    try:
        from src.services.hts_context.config import DEBUG_EXPORT_PATH

        service.export_hierarchy_map()
        print(f"\nHierarchy map exported to: {DEBUG_EXPORT_PATH}")
    except Exception as e:
        print(f"\nWARNING: Could not export hierarchy map: {e}")

    # Final result
    print("\n" + "=" * 80)
    if validation_passed:
        print("VALIDATION RESULT: PASSED")
        print("Service 2 is ready for integration with Service 3")
    else:
        print("VALIDATION RESULT: FAILED")
        print("Please review the errors above")
    print("=" * 80)

    return validation_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
