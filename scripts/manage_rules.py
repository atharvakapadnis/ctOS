"""
Simple CLI tool for managing rules in Service 4
Provides CRUD operations: Create, Read, Update, Delete rules
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.services.rules import RuleManager
from src.services.rules.config import RULES_FILE, ALLOWED_RULE_TYPES


def print_header(text):
    """Print formatted header"""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)


def print_rule(rule):
    """Print a single rule in formatted way"""
    print(f"\n  Rule ID: {rule.rule_id}")
    print(f"  Name: {rule.rule_name}")
    print(f"  Type: {rule.rule_type}")
    print(f"  Status: {'ACTIVE' if rule.active else 'INACTIVE'}")
    print(f"  Content: {rule.rule_content}")
    if rule.description:
        print(f"  Description: {rule.description}")
    if rule.created_at:
        print(f"  Created: {rule.created_at}")
    print("-" * 60)


def initialize_rules_file():
    """Create empty rules.json file if it doesn't exist"""
    if not RULES_FILE.exists():
        print(f"\nCreating new rules file at: {RULES_FILE}")
        RULES_FILE.parent.mkdir(parents=True, exist_ok=True)

        initial_data = {
            "rules": [],
            "metadata": {
                "version": "1.0",
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "total_rules": 0,
                "active_rules": 0,
            },
        }

        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(initial_data, f, indent=2)

        print("Rules file created successfully!")
        return True
    else:
        print(f"\nRules file already exists at: {RULES_FILE}")
        return False


def list_rules():
    """List all rules"""
    print_header("LIST ALL RULES")

    manager = RuleManager()
    rules = manager.load_rules()

    if not rules:
        print("\nNo rules found.")
        return

    stats = manager.get_rules_statistics()
    print(f"\nTotal Rules: {stats['total_rules']}")
    print(f"Active: {stats['active_rules']} | Inactive: {stats['inactive_rules']}")

    print("\n" + "-" * 60)
    for rule in rules:
        print_rule(rule)


def view_rule():
    """View a specific rule by ID"""
    print_header("VIEW RULE")

    rule_id = input("\nEnter Rule ID (e.g., R001): ").strip()

    manager = RuleManager()
    rule = manager.get_rule_by_id(rule_id)

    if rule:
        print_rule(rule)
    else:
        print(f"\nRule '{rule_id}' not found.")


def create_rule():
    """Create a new rule"""
    print_header("CREATE NEW RULE")

    # Get existing rules to determine next ID
    manager = RuleManager()
    existing_rules = manager.load_rules()

    # Suggest next rule ID
    if existing_rules:
        last_id = max([int(r.rule_id[1:]) for r in existing_rules])
        suggested_id = f"R{str(last_id + 1).zfill(3)}"
    else:
        suggested_id = "R001"

    print(f"\nSuggested Rule ID: {suggested_id}")
    rule_id = (
        input(f"Rule ID (press Enter for {suggested_id}): ").strip() or suggested_id
    )

    rule_name = input("Rule Name: ").strip()
    rule_content = input("Rule Content: ").strip()

    print(f"\nAllowed types: {', '.join(ALLOWED_RULE_TYPES)}")
    rule_type = input("Rule Type: ").strip()

    active_input = input("Active? (y/n, default=y): ").strip().lower()
    active = active_input != "n"

    description = input("Description (optional): ").strip()

    # Create rule dictionary
    new_rule = {
        "rule_id": rule_id,
        "rule_name": rule_name,
        "rule_content": rule_content,
        "rule_type": rule_type,
        "active": active,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "description": description if description else None,
    }

    # Validate using RuleManager
    try:
        from src.services.rules.models import Rule

        validated_rule = Rule(**new_rule)

        # Load existing data
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Add new rule
        data["rules"].append(new_rule)

        # Update metadata
        data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        data["metadata"]["total_rules"] = len(data["rules"])
        data["metadata"]["active_rules"] = sum(1 for r in data["rules"] if r["active"])

        # Save
        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print("\n Rule created successfully!")
        print_rule(validated_rule)

    except ValueError as e:
        print(f"\n Validation error: {e}")
        print("Rule not created.")
    except Exception as e:
        print(f"\n Error creating rule: {e}")


def update_rule():
    """Update an existing rule"""
    print_header("UPDATE RULE")

    rule_id = input("\nEnter Rule ID to update: ").strip()

    manager = RuleManager()
    existing_rule = manager.get_rule_by_id(rule_id)

    if not existing_rule:
        print(f"\nRule '{rule_id}' not found.")
        return

    print("\nCurrent rule:")
    print_rule(existing_rule)

    print("\nEnter new values (press Enter to keep current value):")

    rule_name = (
        input(f"Rule Name [{existing_rule.rule_name}]: ").strip()
        or existing_rule.rule_name
    )
    rule_content = (
        input(f"Rule Content [{existing_rule.rule_content[:40]}...]: ").strip()
        or existing_rule.rule_content
    )
    rule_type = (
        input(f"Rule Type [{existing_rule.rule_type}]: ").strip()
        or existing_rule.rule_type
    )

    active_input = (
        input(f"Active? (y/n) [{'y' if existing_rule.active else 'n'}]: ")
        .strip()
        .lower()
    )
    if active_input:
        active = active_input == "y"
    else:
        active = existing_rule.active

    description = input(
        f"Description [{existing_rule.description or 'None'}]: "
    ).strip()
    if not description:
        description = existing_rule.description

    # Update rule dictionary
    updated_rule = {
        "rule_id": rule_id,
        "rule_name": rule_name,
        "rule_content": rule_content,
        "rule_type": rule_type,
        "active": active,
        "created_at": existing_rule.created_at,
        "description": description,
    }

    try:
        from src.services.rules.models import Rule

        validated_rule = Rule(**updated_rule)

        # Load existing data
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Find and update rule
        for i, rule in enumerate(data["rules"]):
            if rule["rule_id"] == rule_id:
                data["rules"][i] = updated_rule
                break

        # Update metadata
        data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        data["metadata"]["active_rules"] = sum(1 for r in data["rules"] if r["active"])

        # Save
        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print("\n Rule updated successfully!")
        print_rule(validated_rule)

    except ValueError as e:
        print(f"\n Validation error: {e}")
    except Exception as e:
        print(f"\n Error updating rule: {e}")


def delete_rule():
    """Delete a rule"""
    print_header("DELETE RULE")

    rule_id = input("\nEnter Rule ID to delete: ").strip()

    manager = RuleManager()
    existing_rule = manager.get_rule_by_id(rule_id)

    if not existing_rule:
        print(f"\nRule '{rule_id}' not found.")
        return

    print("\nRule to delete:")
    print_rule(existing_rule)

    confirm = (
        input("\nAre you sure you want to delete this rule? (yes/no): ").strip().lower()
    )

    if confirm != "yes":
        print("\nDeletion cancelled.")
        return

    try:
        # Load existing data
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Remove rule
        original_count = len(data["rules"])
        data["rules"] = [r for r in data["rules"] if r["rule_id"] != rule_id]

        if len(data["rules"]) == original_count:
            print(f"\nRule '{rule_id}' not found in file.")
            return

        # Update metadata
        data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        data["metadata"]["total_rules"] = len(data["rules"])
        data["metadata"]["active_rules"] = sum(1 for r in data["rules"] if r["active"])

        # Save
        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"\n Rule '{rule_id}' deleted successfully!")

    except Exception as e:
        print(f"\n Error deleting rule: {e}")


def toggle_active_status():
    """Toggle active/inactive status of a rule"""
    print_header("TOGGLE RULE STATUS")

    rule_id = input("\nEnter Rule ID: ").strip()

    manager = RuleManager()
    existing_rule = manager.get_rule_by_id(rule_id)

    if not existing_rule:
        print(f"\nRule '{rule_id}' not found.")
        return

    current_status = "ACTIVE" if existing_rule.active else "INACTIVE"
    new_status = "INACTIVE" if existing_rule.active else "ACTIVE"

    print(f"\nCurrent status: {current_status}")
    print(f"New status: {new_status}")

    confirm = input("\nConfirm toggle? (y/n): ").strip().lower()

    if confirm != "y":
        print("\nToggle cancelled.")
        return

    try:
        # Load existing data
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Find and toggle rule
        for rule in data["rules"]:
            if rule["rule_id"] == rule_id:
                rule["active"] = not rule["active"]
                break

        # Update metadata
        data["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        data["metadata"]["active_rules"] = sum(1 for r in data["rules"] if r["active"])

        # Save
        with open(RULES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"\n Rule '{rule_id}' status changed to {new_status}!")

    except Exception as e:
        print(f"\n Error toggling status: {e}")


def show_statistics():
    """Show rules statistics"""
    print_header("RULES STATISTICS")

    manager = RuleManager()
    stats = manager.get_rules_statistics()

    print(f"\nTotal Rules: {stats['total_rules']}")
    print(f"Active Rules: {stats['active_rules']}")
    print(f"Inactive Rules: {stats['inactive_rules']}")

    if stats["rules_by_type"]:
        print("\nRules by Type:")
        for rule_type, count in sorted(stats["rules_by_type"].items()):
            print(f"  {rule_type}: {count}")


def main_menu():
    """Display main menu and handle user input"""
    while True:
        print_header("RULES MANAGEMENT TOOL")
        print("\n1. List all rules")
        print("2. View specific rule")
        print("3. Create new rule")
        print("4. Update existing rule")
        print("5. Delete rule")
        print("6. Toggle rule status (active/inactive)")
        print("7. Show statistics")
        print("8. Initialize rules file (if not exists)")
        print("9. Exit")

        choice = input("\nEnter choice (1-9): ").strip()

        if choice == "1":
            list_rules()
        elif choice == "2":
            view_rule()
        elif choice == "3":
            create_rule()
        elif choice == "4":
            update_rule()
        elif choice == "5":
            delete_rule()
        elif choice == "6":
            toggle_active_status()
        elif choice == "7":
            show_statistics()
        elif choice == "8":
            initialize_rules_file()
        elif choice == "9":
            print("\nExiting. Goodbye!")
            break
        else:
            print("\nInvalid choice. Please try again.")

        input("\nPress Enter to continue...")


if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        sys.exit(1)
