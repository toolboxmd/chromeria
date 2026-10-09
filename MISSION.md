# Mission

The toolboxmd fork of T3 Code is the user's personal working surface, shaped
around their own tools and habits. It tracks upstream `pingdotgg/t3code` and
absorbs every update by rebasing: fork features live in their own modules and
reach upstream code through small hooks, so no feature is cut to keep the fork
small.

On T3's core the fork adds what the user's tools need instead of rebuilding
harness plumbing: Prism, the fork's router, chooses the model, harness and
effort for every delegated role from measured evidence, falls back when
capacity fails, and runs every agent as a visible T3 thread; OpenBot becomes
a mode switched in the UI, bringing Bots, Channels and the Computer; further
capabilities such as VMs follow. AgentsMD plugins extend the fork rather than
competing with it.
