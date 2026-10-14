# Chromeria map

The parts of Chromeria and how they connect. The Map tab colours each part from
GitHub: planned, working, broken or done.

Each part is declared once as `ID["Name"]`. The comment line `%% ID: refs` under it
names the part's Issues and pull requests; the first ref is the primary ref, which
decides planned, working or done. `%% ID: context` marks a part this project draws
but does not track. GitHub ignores these comment lines.

Change this map in the pull request that adds, splits, merges or removes a part.

```mermaid
flowchart TD
    PZ["Prism routing"]
    %% PZ: #92
    VT["Visible T3 threads"]
    %% VT: #10
    PM["Promachos bubbles and persona"]
    %% PM: #116
    WG["Wight mode"]
    %% WG: #117
    DR["Drafters in any project"]
    %% DR: #114 PR #126
    SC["Spectrum council and free"]
    %% SC: #118
    JT["Spectrum judged tests"]
    %% JT: #119
    OB["OpenBot: Bots, Channels, Computer"]
    %% OB: toolboxmd/openbot PR #142
    UA["Upstream absorption"]
    %% UA: #166
    BR["Bounded event reads"]
    %% BR: #256 PR #267
    PU["Provider updates"]
    %% PU: #263 PR #272
    RS["Resume after restart"]
    %% RS: #264
    SN["Child notices"]
    %% SN: #269 PR #279
    CK["Command keys"]
    %% CK: #232 PR #243
    SP["Shown proof"]
    %% SP: #245
    TF["Test Server 6 flake"]
    %% TF: #259 PR #278
    XM["Cross-machine control"]
    %% XM: #164

    PZ --> PM --> WG
    PZ --> DR --> VT --> XM
    DR --> SC --> JT
    DR --> SN
    VT --> RS
    OB -.-> PM
    UA --> PZ
    CK --- SP
    TF -.-> SN
    BR --- PU
```

## Work that is not a part

| Work               | Why it is not a part                                                                                                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Network routes     | Reaching one environment over several routes already landed from upstream (`979ca66ced`, pingdotgg/t3code#15467). No open Chromeria issue owns a remaining change. |
| OpenBot spike (#7) | Closed as not planned. The OpenBot part points at the direction pull request in toolboxmd/openbot.                                                                 |
| v2 cutover (#177)  | The landing umbrella. Promachos, Wight, Drafters, Spectrum and Prism cite their own issues.                                                                        |
