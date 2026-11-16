/**
 * A searched filter submenu's rows: the first ("all") option, the selection, then the first ten
 * matches by label. `empty` is set when a search leaves nothing but the "all" option.
 */
export function searchFilterOptions<
  Option extends { readonly value: string; readonly label: string },
>(
  options: ReadonlyArray<Option>,
  value: string,
  query: string,
): { readonly shown: ReadonlyArray<Option>; readonly empty: boolean } {
  const needle = query.trim().toLowerCase();
  const rest = options.slice(1);
  const selected = rest.find((option) => option.value === value);
  const matches = rest.filter(
    (option) => option !== selected && option.label.toLowerCase().includes(needle),
  );
  return {
    shown: [...options.slice(0, 1), ...(selected ? [selected] : []), ...matches.slice(0, 10)],
    empty: needle.length > 0 && matches.length === 0 && !selected,
  };
}
