# Chromeria Glossary

The system glossary for Chromeria and its connected projects: AgentsMD, Agent
Observer and ContextMD. Every term two or more of them use is defined here
once; each project's own glossary keeps only its local terms. Themed names
follow the Lightbringer series by Brent Weeks, the source of the company name.

## Chromeria

**Luxin**:
The company; also the work agents produce (code, PRs).

**Chromeria**:
This app and everything built from this fork, including its server on any
machine: the gateway where the user directs every agent across harnesses, with
more interfaces (such as the OpenBot mode) over time.
_Avoid_: T3 Code Fork, Bridge, T3 (for our code)

**T3 Code**:
The upstream project this fork tracks (`pingdotgg/t3code`) and anything built
from it. Short form: T3.

**Promachos**:
The one bot the user talks to: a colleague with its own personality who works
across every Satrapy, does work itself and hands larger chunks to Drafters
through Prism. It acts as the planner when it delegates.
_Avoid_: bot, assistant, Blackguard

**Promachos home**:
The Promachos's own repo and Chromeria project. It holds the persona the
Promachos edits as it learns, and its conversations run there.
_Avoid_: bot home

**Satrapy**:
One area of the user's work, such as a business or a product line, kept in its
own repo with its direction, its Library and its tasks. Shown as Area in the UI.
_Avoid_: business, space, workspace, domain

**Library**:
A project's `library/` folder of ContextMD template items: agent-only knowledge
(facts, procedures, tools) and short entries that point to human-facing files in
`docs/` ("Read docs/adr/0003.md in full").
`docs/`, `GLOSSARY.md` and `docs/adr` are not part of the Library and stay where
they are: human-facing, free form, no template.
_Avoid_: shared folder

**Great Library**:
The library shared between projects, plus personal knowledge outside any project,
kept in the user's private home repository.
_Avoid_: shared layer, global skills

**Wight mode**:
The continuous mode in which the Promachos keeps working one Satrapy on its
own, nudged by continue until its time box or quota line runs out.
_Avoid_: Deep Work, autopilot, always-on loop

**Spectrum**:
A disposable thread where Colors talk with each other, either as a council
or in free mode.
_Avoid_: room, channel, group chat

**Color**:
One harness, model and effort used together, such as Claude Opus 5.5 at high
effort. A Drafter runs on one Color.
_Avoid_: model config, route

**White**:
The Spectrum member that moderates: it writes a council's synthesis, and in
free mode picks each next speaker and writes the closing summary. White runs
on its own Color, set separately from the Colors, and never speaks as one.
_Avoid_: moderator, host

**Drafter**:
Any agent working for the user on one Color, such as one the Promachos hands
work to.
_Avoid_: subagent, helper, bot

**Prism**:
Chromeria's built-in router and the only routing term: it picks the harness,
model and effort for each delegated role and falls back when capacity runs
out. AgentsMD's "routing tool" means Prism here.
_Avoid_: Model Router, router (alone)

**Prism role**:
What delegated work is for, with its own instructions and ordered Colors:
Planner, Researcher, Designer, Worker, Computer use, Roast, Reviewer or Judge.
Promachos is a stored launch kit, not a role `delegate_task` accepts.

**Run group**:
The Drafters one delegation starts on several Colors of a read-only Prism role.
They share the caller's checkout and wake it once when all of them finish; the
role's **Run** says how many Colors one delegation starts.
_Avoid_: fan-out, batch, swarm

**Lane**:
The difficulty of a worker delegation, `easy`, `medium` or `hard`, which Prism
uses to pick its Color.
_Avoid_: tier, track, matcher

**Cue (intent detector)**:
The model that scores ContextMD's `intent` entries at each moment and decides
which ones to serve.
_Avoid_: router, intent router, knowledge router

**Toolbox**:
Planned name for the user's own future harness.

## Connected projects

**AgentsMD**:
The ToolboxMD project that owns the portable agent operating contract and its
installable workflow plugin.
_Avoid_: Matt Skill pack, global prompt repository

**Agent Observer**:
The ToolboxMD tool that measures what agents did on each machine from the
harnesses' own records.

**ContextMD**:
The ToolboxMD project that captures what agents learn and serves it at the
moment it applies. `contextmd` names only its repository, command and paths.
_Avoid_: contextmd (in prose), World Model

**Model Router**:
Retired. The runner that carried one delegated task through dispatcher and
worker threads to a PR; Prism absorbed it.

## Machines and runtime

**Harness**:
The agent program a session runs in: Claude Code, Codex, Grok Build, OpenCode
or Cursor. Agent Observer's harness ids are `claude`, `codex`, `grok`,
`opencode`, and the import sources `t3` and `router`; T3's driver `claudeAgent`
is `claude`. AgentsMD's current flag for it is `--host`; `--harness` is
planned (toolboxmd/agentsmd#213).
_Avoid_: host (for the program), provider (alone)

**Provider instance**:
One configured harness login or account in Chromeria (`providerInstanceId`).
_Avoid_: provider account

**Model vendor**:
The company that serves a model, such as Anthropic, OpenAI or xAI.
_Avoid_: provider (alone)

**Machine**:
One of the user's computers. It holds one Agent Observer ledger and runs one
Chromeria environment.
_Avoid_: host (for a computer), box

**Environment**:
One running Chromeria server and the state it owns (`EnvironmentId`); one per
machine in practice.
_Avoid_: machine (for the server)

**Project**:
One repository; in Chromeria, its registered checkout root (`ProjectId`).
_Avoid_: workspace (for the repository)

**Thread**:
A conversation in Chromeria (`ThreadId`).
_Avoid_: chat

**Top-level thread**:
A thread with no parent, opened by the user or launched on request.
_Avoid_: new chat, separate host task

**Bare launch**:
A launch that strips the context a harness can disable and adds only named content,
with a receipt identifying planned controls, observed evidence and retained native content.
_Avoid_: reduced mode, minimal mode, clean launch

**Session**:
One conversation inside one harness, keyed by the harness's own id, such as
`claude:<id>`. A thread points at its session through `nativeThreadRef`.
_Avoid_: provider session

**Provider session**:
A live or resumable harness process in T3 (`ProviderSession`).
_Avoid_: session (alone)

**Turn**:
One prompt-to-stop cycle in a session (T3 `ProviderTurn`).
_Avoid_: run

**Run**:
One counted user-visible turn on a thread (T3 `Run`). Steering or a retry can
give it several turns; each try is a run attempt (`RunAttempt`).
_Avoid_: turn, pass

**Native execution segment**:
One native child start, resume, workflow member attempt or background operation
and its initiating execution. Assigning its responses also requires a proved
native input or turn boundary.

**Queue**:
Runs waiting on a thread (`t3_queue_*`).
_Avoid_: inbox

**User message**:
One user-role input in a session or thread (T3 `userMessageId`).
_Avoid_: submission, prompt (as a record)

**Created by**:
Who wrote an input. T3 `createdBy` is `user`, `agent` or `system`; Agent
Observer's `created_by` is `user`, `agent` or `unknown`. `system` is T3-only and
`unknown`, meaning evidence is missing, is Observer-only.
_Avoid_: origin, human (Observer's old names, kept as aliases for one release)

## Work and roles

**Task**:
An owned work outcome, tracked by an Issue or PR, that may span sessions,
threads and delegated tasks.
_Avoid_: job, delegated task (for the outcome)

**Tracking reference**:
The host, repository, Issue or PR kind, and number that identify a task across
runs and machines. A delivery PR link does not change that identity.

**Execution purpose**:
The task a run or native execution segment serves, or its explicit unscoped
state. It is independent of delegated task binding and input authorship.
_Avoid_: job ownership, thread ownership (for purpose)

**Delivery relation**:
An explicit declaration that a PR delivers a task's tracking reference. It
neither aliases the references nor changes any execution purpose.

**Delegated task**:
One handoff from a parent to a child thread through `delegate_task` (`taskId`,
`childThreadId`). Always say "delegated task", never "task" alone.
_Avoid_: dispatch

**Planner**:
The agent that owns judgment, specs and acceptance and delegates the rest;
usually the top-level thread, or one launched with the Planner role.
_Avoid_: main agent, coordinator

**Parent**:
The agent that delegated work.
_Avoid_: coordinator

**Child**:
The agent that received delegated work from a parent.
_Avoid_: subthread, sub-coordinator, worker (for any child)

**Worker**:
A child doing implementation work; one of the Prism roles.

**Reviewer**:
A child that reviews a candidate and returns a verdict.
_Avoid_: approver

**Feature map**:
A project's `docs/map.md`: one Mermaid flowchart of its parts. The Map tab shows
author-asserted live capabilities and other-project context, and colours tracked
parts from GitHub.
_Avoid_: status map, roadmap, architecture diagram

**Part**:
One node of a feature map: a piece of the mapped project or its context.
_Avoid_: feature, component, node (for the tracked piece)

**Primary ref**:
The first Issue or PR on a part's ref line; it alone decides planned, working
or done.

**Live part**:
An existing capability in the mapped project, asserted by the map author rather
than inferred from tracked work.

**Context part**:
A part a feature map draws but does not track, because another project owns
it.

## Learning and observation

**Capture**:
ContextMD's whole learning pipeline from sessions to owners. Extract finds
candidate lessons, judge keeps the real ones, classify picks each lesson's
owner and matcher, reflect decides add, update or remove against the owner's
current content, and the write step applies those decisions.
_Avoid_: reflection (for the pipeline)

**Lesson**:
One piece of durable knowledge capture takes from a session, such as a rule,
a fact or a preference.

**Write step**:
The capture step that writes into an owner: code applies the reflected add,
update or remove and commits it.
_Avoid_: apply, reflect

**Owner**:
The file or store that holds and serves one kind of knowledge, such as
preferences, the Promachos persona, a project's `AGENTS.md`, a Library or the
Great Library.
_Avoid_: store (alone)

**Moment**:
A hook point where ContextMD can serve: session start, user message, before a
tool call, tool result, tool failure or before done.
_Avoid_: intervention

**Intervention**:
A user message, interrupt or tool-call rejection in a main session that Agent
Observer selects for a judge.
_Avoid_: moment (Observer's old command name, kept as an alias)

**Correction**:
An intervention that changes what an agent did, as a judge rules it.

**Policy denial**:
A tool call refused by a hook, a permission rule or auto mode, such as a
ContextMD deny. Never an intervention or a correction.
_Avoid_: rejection

**Recurring problem**:
A problem group that a judge's yes answers place in two or more sessions; the
problem and its group exist only on a yes answer.
_Avoid_: repeated correction

**Shown**:
A serve that the harness's own records prove the model received, followed by a
response to that input. Without that proof, the serve is unknown.
_Avoid_: delivered, injected (for this proof)

## Research

**Fetch ladder**:
The ordered ways Chromeria tries to read a research page, stopping when one
returns usable content.
_Avoid_: rung list

**Page cache**:
Chromeria's reusable store of research pages and their provenance, shared by
threads that use the same research environment.
_Avoid_: scrape cache
