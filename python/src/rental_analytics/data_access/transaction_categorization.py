"""Transaction categorization using crosswalk reference data.

This module loads and combines crosswalk files to categorize bank transaction
descriptions into standardized categories.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Set
import re


def normalize_pattern(text: str) -> str:
    """
    Normalize text for matching by removing spaces and converting to uppercase.
    
    This ensures that patterns like "DIRECTDEBITCODEPTREVENUETAXPAYMENT" will match
    descriptions like "Direct Debit COD EPT Revenue Tax Payment" or "direct debit cod ept revenue tax payment".
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text (uppercase, no spaces)
    """
    if pd.isna(text) or not text:
        return ""
    return re.sub(r'\s+', '', str(text).strip().upper())


def load_crosswalks() -> Dict[str, pd.DataFrame]:
    """
    Load all crosswalk files from the Reference directory.

    Returns:
        Dictionary mapping crosswalk names to dataframes
    """
    # Get project root (5 levels up from this file)
    project_root = Path(__file__).parent.parent.parent.parent.parent
    reference_dir = project_root / 'data' / 'Reference'

    crosswalks = {}

    # Load BMO crosswalk
    bmo_file = reference_dir / 'BMO 2024 tx cat xwalk.csv'
    if bmo_file.exists():
        try:
            bmo_df = pd.read_csv(bmo_file)
            # Ensure column names are standardized
            if 'search_pattern' in bmo_df.columns and 'category' in bmo_df.columns:
                crosswalks['bmo'] = bmo_df[['search_pattern', 'category']].copy()
            else:
                # Try without header if first row looks like data
                bmo_df = pd.read_csv(bmo_file, header=None, names=['search_pattern', 'category'])
                crosswalks['bmo'] = bmo_df
        except Exception as e:
            print(f"Warning: Could not load BMO crosswalk: {e}")

    # Load BotW crosswalks
    botw_files = [
        reference_dir / 'BotW desc cat xwalk.csv',
        reference_dir / 'BotW desc cat xwalk_2023Partnership.csv',
    ]

    botw_dfs = []
    for botw_file in botw_files:
        if botw_file.exists():
            try:
                botw_df = pd.read_csv(botw_file)
                # Check if it has headers
                if 'search_pattern' in botw_df.columns and 'category' in botw_df.columns:
                    botw_dfs.append(botw_df[['search_pattern', 'category']].copy())
                else:
                    # Try without header
                    botw_df = pd.read_csv(botw_file, header=None, names=['search_pattern', 'category'])
                    botw_dfs.append(botw_df)
            except Exception as e:
                print(f"Warning: Could not load {botw_file.name}: {e}")

    # Combine BotW crosswalks (remove duplicates, keep first occurrence)
    if botw_dfs:
        combined_botw = pd.concat(botw_dfs, ignore_index=True)
        combined_botw = combined_botw.drop_duplicates(subset=['search_pattern'], keep='first')
        crosswalks['botw'] = combined_botw

    return crosswalks


def combine_crosswalks(crosswalks: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
    """
    Combine all crosswalks into a single unified mapping.

    Args:
        crosswalks: Optional pre-loaded crosswalks dict. If None, loads from files.

    Returns:
        Combined dataframe with search_pattern and category columns,
        plus a bank_source column indicating which bank the pattern applies to
    """
    if crosswalks is None:
        crosswalks = load_crosswalks()

    combined_dfs = []

    for bank_source, df in crosswalks.items():
        df_copy = df.copy()
        df_copy['bank_source'] = bank_source.upper()
        combined_dfs.append(df_copy)

    if not combined_dfs:
        # Return empty dataframe with correct structure
        return pd.DataFrame(columns=['search_pattern', 'category', 'bank_source'])

    combined = pd.concat(combined_dfs, ignore_index=True)

    # Remove duplicates (same pattern and category), keeping first occurrence
    combined = combined.drop_duplicates(subset=['search_pattern', 'category'], keep='first')

    return combined


def create_categorization_mapping(crosswalks_df: Optional[pd.DataFrame] = None, use_normalized: bool = True) -> Dict[str, str]:
    """
    Create a dictionary mapping search patterns to categories.

    Args:
        crosswalks_df: Optional combined crosswalks dataframe. If None, loads and combines.
        use_normalized: If True, normalize patterns (strip spaces, uppercase) for matching.
                       If False, use patterns as-is.

    Returns:
        Dictionary mapping normalized search_pattern -> category
    """
    if crosswalks_df is None:
        crosswalks_df = combine_crosswalks()

    # Create mapping dictionary
    # If multiple categories exist for same pattern, take the first one
    mapping = {}
    for _, row in crosswalks_df.iterrows():
        if use_normalized:
            pattern = normalize_pattern(row['search_pattern'])
        else:
            pattern = str(row['search_pattern']).strip().upper()
        category = str(row['category']).strip()
        if pattern and pattern not in mapping:
            mapping[pattern] = category

    return mapping


def categorize_transaction(
    description: str,
    bank_source: str,
    crosswalks_df: Optional[pd.DataFrame] = None,
    mapping: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Categorize a single transaction description.

    Uses normalized matching (space-stripped, uppercase) to ensure patterns
    like "DIRECTDEBITCODEPTREVENUETAXPAYMENT" match descriptions like
    "Direct Debit COD EPT Revenue Tax Payment".

    Args:
        description: Transaction description text
        bank_source: Bank source ('BMO' or 'BotW')
        crosswalks_df: Optional combined crosswalks dataframe
        mapping: Optional pre-computed mapping dictionary (should use normalized patterns)

    Returns:
        Category string if match found, None otherwise
    """
    if pd.isna(description) or not description:
        return None

    if mapping is None:
        if crosswalks_df is None:
            crosswalks_df = combine_crosswalks()
        mapping = create_categorization_mapping(crosswalks_df, use_normalized=True)

    # Normalize description for matching (strip spaces, uppercase)
    desc_normalized = normalize_pattern(description)

    # First, try exact match on normalized description
    if desc_normalized in mapping:
        return mapping[desc_normalized]

    # Then try substring match (normalized)
    for pattern, category in mapping.items():
        if pattern in desc_normalized:
            return category

    return None


def categorize_transactions_df(
    df: pd.DataFrame,
    description_col: str = 'description',
    bank_source_col: str = 'bank_source',
    category_col: str = 'category',
    crosswalks_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Add category column to transaction dataframe based on description matching.

    Uses normalized matching (space-stripped, uppercase) to ensure patterns match
    descriptions regardless of spacing and case.

    Args:
        df: Transaction dataframe
        description_col: Name of description column
        bank_source_col: Name of bank source column
        category_col: Name to use for the new category column
        crosswalks_df: Optional combined crosswalks dataframe

    Returns:
        Dataframe with added category column
    """
    result_df = df.copy()

    # Load crosswalks if not provided
    if crosswalks_df is None:
        crosswalks_df = combine_crosswalks()

    # Create mapping for efficiency (using normalized patterns)
    mapping = create_categorization_mapping(crosswalks_df, use_normalized=True)

    # Apply categorization
    def categorize_row(row):
        desc = row.get(description_col, '')
        bank = row.get(bank_source_col, '')
        return categorize_transaction(desc, bank, mapping=mapping)

    result_df[category_col] = result_df.apply(categorize_row, axis=1)

    return result_df


def get_category_summary(
    df: pd.DataFrame,
    category_col: str = 'category',
    bank_source_col: str = 'bank_source'
) -> pd.DataFrame:
    """
    Generate summary statistics by category.

    Args:
        df: Transaction dataframe with category column
        category_col: Name of category column
        bank_source_col: Name of bank source column

    Returns:
        Summary dataframe with counts and amounts by category
    """
    if category_col not in df.columns:
        raise ValueError(f"Category column '{category_col}' not found in dataframe")

    summary = df.groupby([category_col, bank_source_col]).agg({
        'amount': ['count', 'sum', 'mean'],
    }).reset_index()

    summary.columns = [category_col, bank_source_col, 'transaction_count', 'total_amount', 'mean_amount']

    # Also get overall summary (all banks combined)
    overall = df.groupby(category_col).agg({
        'amount': ['count', 'sum', 'mean'],
    }).reset_index()
    overall.columns = [category_col, 'transaction_count', 'total_amount', 'mean_amount']
    overall[bank_source_col] = 'ALL'

    # Combine and sort
    combined = pd.concat([summary, overall], ignore_index=True)
    combined = combined.sort_values([category_col, bank_source_col])

    return combined


def add_unknown_patterns(
    df: pd.DataFrame,
    description_col: str = 'description',
    category_col: str = 'category',
    crosswalks_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Identify transactions with unknown patterns and add them to the crosswalk.

    This function finds transactions that don't have a category assigned and
    allows you to add new search patterns to the crosswalk.

    Args:
        df: Transaction dataframe with description and category columns
        description_col: Name of description column
        category_col: Name of category column
        crosswalks_df: Optional combined crosswalks dataframe to update

    Returns:
        Updated crosswalks dataframe with new patterns added
    """
    if crosswalks_df is None:
        crosswalks_df = combine_crosswalks()

    # Find transactions without categories
    unknown_df = df[df[category_col].isna() | (df[category_col] == '')].copy()
    
    if unknown_df.empty:
        return crosswalks_df

    # Get unique unknown descriptions
    unknown_descriptions = unknown_df[description_col].dropna().unique()
    
    # Create new entries for unknown patterns
    # Get set of normalized existing patterns for efficient lookup
    existing_normalized = set(crosswalks_df['search_pattern'].apply(normalize_pattern).values)
    
    new_patterns = []
    for desc in unknown_descriptions:
        if desc:
            # Use normalized pattern (space-stripped, uppercase) as the search pattern
            normalized_pattern = normalize_pattern(desc)
            # Check if this pattern already exists (using normalized comparison)
            if normalized_pattern not in existing_normalized:
                new_patterns.append({
                    'search_pattern': normalized_pattern,
                    'category': 'UNKNOWN',  # Default category, user should update
                    'bank_source': 'UNKNOWN'
                })
                existing_normalized.add(normalized_pattern)  # Avoid duplicates in this batch

    if new_patterns:
        new_df = pd.DataFrame(new_patterns)
        crosswalks_df = pd.concat([crosswalks_df, new_df], ignore_index=True)
        # Remove duplicates, keeping first occurrence
        crosswalks_df = crosswalks_df.drop_duplicates(subset=['search_pattern'], keep='first')

    return crosswalks_df


def add_category_for_pattern(
    crosswalks_df: pd.DataFrame,
    search_pattern: str,
    category: str,
    bank_source: Optional[str] = None
) -> pd.DataFrame:
    """
    Add or update a category for a specific search pattern.

    Args:
        crosswalks_df: Crosswalks dataframe to update
        search_pattern: Search pattern to add/update (will be normalized)
        category: Category to assign
        bank_source: Optional bank source (defaults to 'UNKNOWN')

    Returns:
        Updated crosswalks dataframe
    """
    crosswalks_df = crosswalks_df.copy()
    normalized_pattern = normalize_pattern(search_pattern)
    
    if bank_source is None:
        bank_source = 'UNKNOWN'
    
    # Check if pattern already exists
    pattern_mask = crosswalks_df['search_pattern'].apply(normalize_pattern) == normalized_pattern
    
    if pattern_mask.any():
        # Update existing pattern
        crosswalks_df.loc[pattern_mask, 'category'] = category
        crosswalks_df.loc[pattern_mask, 'bank_source'] = bank_source.upper()
    else:
        # Add new pattern
        new_row = pd.DataFrame({
            'search_pattern': [normalized_pattern],
            'category': [category],
            'bank_source': [bank_source.upper()]
        })
        crosswalks_df = pd.concat([crosswalks_df, new_row], ignore_index=True)
    
    return crosswalks_df


def interactively_categorize_unknowns(
    df: pd.DataFrame,
    description_col: str = 'description',
    bank_source_col: str = 'bank_source',
    category_col: str = 'category',
    crosswalks_df: Optional[pd.DataFrame] = None,
    auto_save: bool = True,
    show_existing_categories: bool = True
) -> pd.DataFrame:
    """
    Interactively prompt user to categorize unknown transaction patterns.
    
    This function works in any shell (zsh, bash, etc.) and prompts the user
    when unknown patterns are encountered, allowing them to:
    - Assign a category to the pattern
    - Skip the pattern (leave as UNKNOWN)
    - View existing categories for reference
    
    Args:
        df: Transaction dataframe with description and category columns
        description_col: Name of description column
        bank_source_col: Name of bank source column
        category_col: Name of category column
        crosswalks_df: Optional combined crosswalks dataframe to update
        auto_save: If True, automatically saves crosswalk after each assignment
        show_existing_categories: If True, shows existing categories for reference
    
    Returns:
        Updated crosswalks dataframe with new categorizations
    """
    if crosswalks_df is None:
        crosswalks_df = combine_crosswalks()
    
    # Find transactions without categories
    unknown_df = df[df[category_col].isna() | (df[category_col] == '')].copy()
    
    if unknown_df.empty:
        print("No unknown patterns found. All transactions are categorized.")
        return crosswalks_df
    
    # Get unique unknown descriptions with their bank sources
    unknown_info = unknown_df.groupby([description_col, bank_source_col]).size().reset_index(name='count')
    unknown_info = unknown_info.sort_values('count', ascending=False)
    
    # Get existing categories for reference
    existing_categories = sorted(crosswalks_df['category'].unique().tolist()) if show_existing_categories else []
    
    print(f"\n{'='*70}")
    print(f"INTERACTIVE CATEGORIZATION")
    print(f"{'='*70}")
    print(f"Found {len(unknown_info)} unique unknown patterns")
    print(f"Total unknown transactions: {unknown_info['count'].sum()}")
    print(f"\nCommands:")
    print(f"  - Enter a category name to assign it")
    print(f"  - Type 'skip' or 's' to skip this pattern")
    print(f"  - Type 'list' or 'l' to see existing categories")
    print(f"  - Type 'quit' or 'q' to stop and save progress")
    print(f"{'='*70}\n")
    
    updated_crosswalks = crosswalks_df.copy()
    processed = 0
    skipped = 0
    categorized = 0
    should_quit = False
    
    for idx, row in unknown_info.iterrows():
        if should_quit:
            break
            
        desc = row[description_col]
        bank = row[bank_source_col]
        count = row['count']
        normalized_pattern = normalize_pattern(desc)
        
        # Check if this pattern was already added during this session
        existing_normalized = set(updated_crosswalks['search_pattern'].apply(normalize_pattern).values)
        if normalized_pattern in existing_normalized:
            # Check if it's still UNKNOWN
            pattern_mask = updated_crosswalks['search_pattern'].apply(normalize_pattern) == normalized_pattern
            if pattern_mask.any():
                current_category = updated_crosswalks.loc[pattern_mask, 'category'].iloc[0]
                if current_category != 'UNKNOWN':
                    continue  # Already categorized, skip
        
        print(f"\n[{processed + 1}/{len(unknown_info)}] Pattern: {normalized_pattern}")
        print(f"  Description: {desc}")
        print(f"  Bank: {bank}")
        print(f"  Occurrences: {count}")
        
        while True:
            user_input = input("  Enter category (or 'skip'/'list'/'quit'): ").strip()
            
            if user_input.lower() in ['quit', 'q']:
                print(f"\nStopping categorization. Processed {processed} patterns.")
                print(f"  - Categorized: {categorized}")
                print(f"  - Skipped: {skipped}")
                should_quit = True
                break
            
            elif user_input.lower() in ['list', 'l']:
                if existing_categories:
                    print(f"\n  Existing categories ({len(existing_categories)}):")
                    for i, cat in enumerate(existing_categories, 1):
                        print(f"    {i}. {cat}")
                    print()
                else:
                    print("  No existing categories found.\n")
                continue
            
            elif user_input.lower() in ['skip', 's']:
                # Add as UNKNOWN if not already in crosswalk
                if normalized_pattern not in existing_normalized:
                    new_row = pd.DataFrame({
                        'search_pattern': [normalized_pattern],
                        'category': ['UNKNOWN'],
                        'bank_source': [bank.upper() if pd.notna(bank) else 'UNKNOWN']
                    })
                    updated_crosswalks = pd.concat([updated_crosswalks, new_row], ignore_index=True)
                    existing_normalized.add(normalized_pattern)
                skipped += 1
                processed += 1
                break
            
            elif user_input:
                # Assign the category
                category = user_input.strip()
                
                # Add to existing categories list if new
                if category not in existing_categories:
                    existing_categories.append(category)
                    existing_categories.sort()
                
                # Update or add the pattern
                pattern_mask = updated_crosswalks['search_pattern'].apply(normalize_pattern) == normalized_pattern
                
                if pattern_mask.any():
                    # Update existing
                    updated_crosswalks.loc[pattern_mask, 'category'] = category
                    updated_crosswalks.loc[pattern_mask, 'bank_source'] = bank.upper() if pd.notna(bank) else 'UNKNOWN'
                else:
                    # Add new
                    new_row = pd.DataFrame({
                        'search_pattern': [normalized_pattern],
                        'category': [category],
                        'bank_source': [bank.upper() if pd.notna(bank) else 'UNKNOWN']
                    })
                    updated_crosswalks = pd.concat([updated_crosswalks, new_row], ignore_index=True)
                    existing_normalized.add(normalized_pattern)
                
                categorized += 1
                processed += 1
                print(f"  ✓ Assigned category '{category}' to pattern '{normalized_pattern}'")
                
                # Auto-save if enabled
                if auto_save:
                    try:
                        export_finalized_crosswalk(updated_crosswalks, include_bank_source=True)
                        print(f"  ✓ Auto-saved crosswalk")
                    except Exception as e:
                        print(f"  ⚠ Warning: Could not auto-save: {e}")
                
                break
            
            else:
                print("  Please enter a category name, 'skip', 'list', or 'quit'")
    
    # Final save
    if processed > 0:
        try:
            export_finalized_crosswalk(updated_crosswalks, include_bank_source=True)
            print(f"\n✓ Final crosswalk saved to data/output/finalized_crosswalk.csv")
        except Exception as e:
            print(f"\n⚠ Warning: Could not save final crosswalk: {e}")
    
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"Total processed: {processed}")
    print(f"  - Categorized: {categorized}")
    print(f"  - Skipped: {skipped}")
    print(f"{'='*70}\n")
    
    return updated_crosswalks


def export_finalized_crosswalk(
    crosswalks_df: Optional[pd.DataFrame] = None,
    filename: str = 'finalized_crosswalk.csv',
    include_bank_source: bool = False
) -> Path:
    """
    Export the finalized crosswalk to a CSV file in the output directory.

    Args:
        crosswalks_df: Optional combined crosswalks dataframe. If None, loads and combines.
        filename: Name of the output CSV file
        include_bank_source: If True, include bank_source column in output

    Returns:
        Path to the exported CSV file
    """
    if crosswalks_df is None:
        crosswalks_df = combine_crosswalks()

    # Get project root and output directory
    project_root = Path(__file__).parent.parent.parent.parent.parent
    output_dir = project_root / 'data' / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Prepare export dataframe
    export_df = crosswalks_df.copy()
    
    # Sort by category and search_pattern for easier review
    export_df = export_df.sort_values(['category', 'search_pattern'])
    
    # Select columns to export
    if include_bank_source:
        export_columns = ['search_pattern', 'category', 'bank_source']
    else:
        export_columns = ['search_pattern', 'category']
        # If bank_source exists but not included, we might want to note conflicts
        if 'bank_source' in export_df.columns:
            # Check for patterns with multiple bank sources
            pattern_banks = export_df.groupby('search_pattern')['bank_source'].apply(
                lambda x: '|'.join(sorted(x.unique()))
            )
            multi_bank_patterns = pattern_banks[pattern_banks.str.contains('\|')]
            if not multi_bank_patterns.empty:
                print(f"Note: {len(multi_bank_patterns)} patterns have multiple bank sources")

    export_df = export_df[export_columns]

    # Export to CSV
    output_path = output_dir / filename
    export_df.to_csv(output_path, index=False)
    
    print(f"Exported finalized crosswalk to: {output_path}")
    print(f"Total patterns: {len(export_df)}")
    print(f"Unique categories: {export_df['category'].nunique()}")
    
    return output_path


if __name__ == "__main__":
    # Example usage
    print("Loading crosswalks...")
    crosswalks = load_crosswalks()
    print(f"Loaded {len(crosswalks)} crosswalk file(s)")

    print("\nCombining crosswalks...")
    combined = combine_crosswalks(crosswalks)
    print(f"Combined crosswalk has {len(combined)} patterns")
    print(f"\nSample patterns:")
    print(combined.head(10))

    print(f"\nUnique categories: {combined['category'].nunique()}")
    print(f"\nCategory distribution:")
    print(combined['category'].value_counts().head(10))
    
    # Test normalized matching
    print("\n" + "="*60)
    print("Testing normalized matching...")
    test_descriptions = [
        "Direct Debit COD EPT Revenue Tax Payment",
        "direct debit cod ept revenue tax payment",
        "DIRECTDEBITCODEPTREVENUETAXPAYMENT",
        "DirectDebitCODeptRevenueTaxPayment"
    ]
    mapping = create_categorization_mapping(combined, use_normalized=True)
    for desc in test_descriptions:
        category = categorize_transaction(desc, "BMO", mapping=mapping)
        normalized = normalize_pattern(desc)
        print(f"  '{desc}' -> normalized: '{normalized}' -> category: {category}")
    
    # Export finalized crosswalk
    print("\n" + "="*60)
    print("Exporting finalized crosswalk...")
    export_path = export_finalized_crosswalk(combined, include_bank_source=True)
    print(f"Exported to: {export_path}")

