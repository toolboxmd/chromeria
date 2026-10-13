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
A Satrapy's own skills, playbooks and tools, kept in that Satrapy's repo.
_Avoid_: shared folder

**Great Library**:
Skills, playbooks and tools used by two or more Satrapies, kept once and
available to every Satrapy.
_Avoid_: shared layer, global skills

**Wight mode**:
The continuous mode in which the Promachos keeps working one Satrapy on its
own, nudged by continue until its time box or quota line runs out.
_Avoid_: Deep Work, autopilot, always-on loop

**Spectrum**:
A disposable thread where Drafters of several Colors talk with each other,
either as a moderated council or freely.
_Avoid_: room, channel, group chat

**Color**:
One harness, model and effort used together, such as Claude Opus 5.5 at high
effort. A Drafter runs on one Color.
_Avoid_: model config, route

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
A kit Prism routes for: promachos, planner, dispatcher, reviewer, worker, retry
or escalation.

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

**Delegated task**:
One handoff from a parent to a child thread through `delegate_task` (`taskId`,
`childThreadId`). Always say "delegated task", never "task" alone.
_Avoid_: dispatch

**Planner**:
The top-level agent the user works with. It owns judgment, specs and
acceptance, and delegates the rest.
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
