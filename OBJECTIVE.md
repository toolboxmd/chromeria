# Objective

Make the fork maintainable and bring Model Router into it.

The Objective is complete when:

- A documented rebase routine keeps the fork's changes as a small commit
  stack on upstream `main`, with CI, and one upstream update has been
  absorbed through it.
- A planner hands each PR job to a dispatcher thread by default, or
  coordinates it itself when asked; both start workers and a reviewer with
  `spawn_thread`, Prism picks each role's model, and one real task per mode
  reaches an open, independently reviewed PR without the Python runner.
- Agent Observer compares the planner dispatching itself with a dispatcher
  thread on matched real jobs (time to first worker, time to a reviewed PR,
  success rate, tokens and cost), and the faster reliable mode becomes the
  default.
- Prism roles use the same six names everywhere and carry only models and
  instructions; every role has every thread tool.
- The Python runner is retired: its Chromeria hooks are removed and
  `toolboxmd/model-router` is archived.
- An OpenBot-on-T3 spike measures the Computer as a right-panel tab and a Bot
  as a thread with a persona, with its upstream edit count.

The OpenBot mode UI, VMs, mobile builds and multi-machine operation remain
outside this Objective.
