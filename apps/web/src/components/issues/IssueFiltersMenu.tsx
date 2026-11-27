import {
  ISSUE_GROUPS,
  type GitHubIssueListSort,
  type GitHubIssueListState,
  type IssueGroup,
} from "@t3tools/contracts";
import {
  ArrowDownUpIcon,
  CircleDotDashedIcon,
  CircleSlashIcon,
  FolderGit2Icon,
  LayersIcon,
  MessageSquareIcon,
  MessageSquareOffIcon,
  MilestoneIcon,
} from "lucide-react";
import { msg, T, useGT, useMessages } from "gt-react";

import {
  PullRequestFilterRadioGroup,
  PullRequestFilterRadioSubmenu,
  PullRequestFiltersTrigger,
  PullRequestLabelFilter,
  PullRequestSearchableFilterSubmenu,
  type PullRequestFilterOption,
} from "../pullRequest/PullRequestListFilters";
import { resolveFilterOptions } from "../pullRequest/pullRequestListOptions";
import { Button } from "../ui/button";
import {
  Menu,
  MenuCheckboxItem,
  MenuItem,
  MenuPopup,
  MenuSeparator,
  MenuSub,
  MenuSubPopup,
  MenuSubTrigger,
  MenuTrigger,
} from "../ui/menu";
import { issueKey, type IssueFacets, type IssueListFilters } from "./issueList.logic";
import {
  ISSUE_GROUP_PRESENTATION,
  ISSUE_STATE_PRESENTATION,
  IssueGroupGlyph,
} from "./issuePresentation";
import type { IssueGroupFilters } from "./issueStatus.logic";

export type IssueMenuFilters = IssueListFilters & IssueGroupFilters;

/** The unset value of each single-choice filter. */
const ANY = "";

export const ISSUE_STATE_OPTIONS = [
  {
    value: "open",
    label: msg("Open", { $context: "issue state filter" }),
    Icon: ISSUE_STATE_PRESENTATION.open.Icon,
  },
  {
    value: "closed",
    label: msg("Closed", { $context: "issue state filter" }),
    Icon: ISSUE_STATE_PRESENTATION.done.Icon,
  },
  { value: "all", label: msg("All", { $context: "issue state filter" }), Icon: LayersIcon },
] as const satisfies ReadonlyArray<PullRequestFilterOption<GitHubIssueListState>>;

const LINKED_OPTIONS = [
  { value: ANY, label: msg("Linked or not"), Icon: LayersIcon },
  { value: "linked", label: msg("Linked to a thread"), Icon: MessageSquareIcon },
  { value: "unlinked", label: msg("Not linked"), Icon: MessageSquareOffIcon },
] as const satisfies ReadonlyArray<PullRequestFilterOption<string>>;

/** Issues' Filters menu, built from the pull request list's rows and submenus. */
export function IssueFiltersMenu({
  state,
  onState,
  filters,
  onFilters,
  facets,
  repositories,
}: {
  state: GitHubIssueListState;
  onState: (state: GitHubIssueListState) => void;
  filters: IssueMenuFilters;
  onFilters: (update: (previous: IssueMenuFilters) => IssueMenuFilters) => void;
  facets: IssueFacets;
  repositories: ReadonlyArray<{
    readonly host: string;
    readonly repository: string;
    readonly projectTitle: string;
  }>;
}) {
  const gt = useGT();
  const messages = useMessages();
  const filterCount =
    (state === "open" ? 0 : 1) +
    (filters.repository ? 1 : 0) +
    (filters.labels?.length ?? 0) +
    (filters.milestone ? 1 : 0) +
    (filters.parent ? 1 : 0) +
    (filters.groups?.length ?? 0) +
    (filters.linked ? 1 : 0);
  const set = (patch: Partial<IssueMenuFilters>) =>
    onFilters((previous) => ({ ...previous, ...patch }));
  const orUndefined = (value: string) => (value === ANY ? undefined : value);
  const repositoryOptions: ReadonlyArray<PullRequestFilterOption<string>> = [
    { value: ANY, label: gt("All projects"), Icon: LayersIcon },
    ...repositories.map((repository) => ({
      value: `${repository.host} ${repository.repository}`,
      label: `${repository.projectTitle} · ${repository.repository}`,
      Icon: FolderGit2Icon,
    })),
  ];
  const milestoneOptions: ReadonlyArray<PullRequestFilterOption<string>> = [
    { value: ANY, label: gt("Any milestone"), Icon: LayersIcon },
    // An active milestone stays listed after the loaded rows stop carrying it.
    ...[
      ...facets.milestones,
      ...(filters.milestone && !facets.milestones.includes(filters.milestone)
        ? [filters.milestone]
        : []),
    ].map((milestone) => ({ value: milestone, label: milestone, Icon: MilestoneIcon })),
  ];
  const parentOptions: ReadonlyArray<PullRequestFilterOption<string>> = [
    { value: ANY, label: gt("Any parent"), Icon: LayersIcon },
    { value: "none", label: gt("No parent"), Icon: CircleSlashIcon },
    ...facets.parents.map((parent) => ({
      value: issueKey(parent),
      label: `#${parent.number} ${parent.title}`,
      Icon: ISSUE_STATE_PRESENTATION[parent.state].Icon,
    })),
    // Likewise an active parent, named by its number from the key.
    ...(filters.parent &&
    filters.parent !== "none" &&
    !facets.parents.some((parent) => issueKey(parent) === filters.parent)
      ? [
          {
            value: filters.parent,
            label: `#${filters.parent.split("#").at(-1)}`,
            Icon: ISSUE_STATE_PRESENTATION.open.Icon,
          },
        ]
      : []),
  ];
  return (
    <Menu>
      <PullRequestFiltersTrigger count={filterCount} />
      <MenuPopup align="end" side="bottom">
        <PullRequestFilterRadioSubmenu
          label={gt("State", { $context: "issue filter" })}
          value={state}
          options={resolveFilterOptions(ISSUE_STATE_OPTIONS, messages)}
          onChange={onState}
        />
        <IssueGroupFilter value={filters.groups ?? []} onChange={(groups) => set({ groups })} />
        <MenuSeparator />
        <PullRequestSearchableFilterSubmenu
          label={gt("Project", { $context: "issue filter" })}
          value={filters.repository ?? ANY}
          options={repositoryOptions}
          onChange={(repository) => set({ repository: orUndefined(repository) })}
          searchLabel={gt("Search projects")}
        />
        <PullRequestLabelFilter
          value={filters.labels ?? []}
          options={facets.labels}
          scope={[]}
          onChange={(labels) => set({ labels: labels.slice(0, 10) })}
        />
        <PullRequestFilterRadioSubmenu
          label={gt("Milestone", { $context: "issue filter" })}
          value={filters.milestone ?? ANY}
          options={milestoneOptions}
          onChange={(milestone) => set({ milestone: orUndefined(milestone) })}
        />
        <PullRequestSearchableFilterSubmenu
          label={gt("Parent", { $context: "issue filter, parent issue" })}
          value={filters.parent ?? ANY}
          options={parentOptions}
          onChange={(parent) => set({ parent: orUndefined(parent) })}
          searchLabel={gt("Search parents")}
        />
        <PullRequestFilterRadioSubmenu
          label={gt("Linked", { $context: "issue filter, linked to a thread" })}
          value={filters.linked ?? ANY}
          options={resolveFilterOptions(LINKED_OPTIONS, messages)}
          onChange={(linked) =>
            set({ linked: linked === "linked" || linked === "unlinked" ? linked : undefined })
          }
        />
        {filterCount > 0 ? (
          <>
            <MenuSeparator />
            <MenuItem
              onClick={() => {
                onFilters(() => ({}));
                if (state !== "open") onState("open");
              }}
            >
              <T>Clear filters</T>
            </MenuItem>
          </>
        ) : null}
      </MenuPopup>
    </Menu>
  );
}

/** Any of several groups, by whose move it is; unset shows every one. */
function IssueGroupFilter({
  value,
  onChange,
}: {
  value: ReadonlyArray<IssueGroup>;
  onChange: (groups: ReadonlyArray<IssueGroup>) => void;
}) {
  const gt = useGT();
  const messages = useMessages();
  const only = value.length === 1 ? value[0] : undefined;
  return (
    <MenuSub>
      <MenuSubTrigger>
        {only ? (
          <IssueGroupGlyph group={only} className="size-3.5" />
        ) : (
          <CircleDotDashedIcon aria-hidden className="size-3.5" />
        )}
        <span className="flex-1">
          <T context="issue filter">Status</T>
        </span>
        <span className="min-w-0 max-w-32 truncate text-xs text-muted-foreground">
          {only
            ? messages(ISSUE_GROUP_PRESENTATION[only].label)
            : value.length === 0
              ? gt("Any", { $context: "no status filter set" })
              : gt("{count, plural, one {{count} selected} other {{count} selected}}", {
                  count: value.length,
                })}
        </span>
      </MenuSubTrigger>
      <MenuSubPopup>
        {ISSUE_GROUPS.map((group) => (
          <MenuCheckboxItem
            key={group}
            checked={value.includes(group)}
            closeOnClick={false}
            onCheckedChange={(next) => {
              const others = value.filter((candidate) => candidate !== group);
              onChange(next ? [...others, group] : others);
            }}
          >
            <span className="flex min-w-0 items-center gap-2">
              <IssueGroupGlyph group={group} className="size-3.5" />
              <span className="min-w-0 flex-1 truncate">
                {messages(ISSUE_GROUP_PRESENTATION[group].label)}
              </span>
            </span>
          </MenuCheckboxItem>
        ))}
      </MenuSubPopup>
    </MenuSub>
  );
}

/** The Sort button, shaped like the pull request list's. */
export function IssueSortMenu<Value extends GitHubIssueListSort>({
  value,
  options,
  onChange,
}: {
  value: Value;
  options: ReadonlyArray<PullRequestFilterOption<Value>>;
  onChange: (sort: Value) => void;
}) {
  const gt = useGT();
  const current = options.find((option) => option.value === value);
  return (
    <Menu>
      <MenuTrigger
        aria-label={gt("Sort Issues: {sort}", { sort: current?.label ?? "" })}
        render={<Button variant="outline" />}
      >
        <ArrowDownUpIcon aria-hidden className="size-4" />
        <span>
          <T context="verb, opens sort options">Sort</T>
        </span>
      </MenuTrigger>
      <MenuPopup align="start" side="bottom">
        <PullRequestFilterRadioGroup
          label={gt("Sort by")}
          value={value}
          options={options}
          onChange={onChange}
        />
      </MenuPopup>
    </Menu>
  );
}
