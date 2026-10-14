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
    BL["Bare launch and receipts"]
    %% BL: #289, PR #335
    DR["Drafters in any project"]
    %% DR: #114 PR #126
    SC["Spectrum council and free"]
    %% SC: #118
    JT["Spectrum judged tests"]
    %% JT: #119
    OB["OpenBot: Bots, Channels, Computer"]
    %% OB: context
    UA["Upstream absorption"]
    %% UA: #166 PR #291
    BR["Bounded event reads"]
    %% BR: #256 PR #267
    PU["Provider updates"]
    %% PU: #263 PR #272
    RS["Resume after restart"]
    %% RS: #264 PR #301 PR #291 PR #329
    SN["Child notices"]
    %% SN: #269 PR #279
    CK["Command keys"]
    %% CK: #232 PR #243
    SP["Shown proof"]
    %% SP: #245 PR #313
    TF["Test Server 6 flake"]
    %% TF: #259 PR #278
    XM["Cross-machine control"]
    %% XM: #164 PR #291 PR #290 PR #303 #302 PR #304
    NR["Network routes"]
    %% NR: #292
    PRN["Paired session renewal"]
    %% PRN: #323 PR #334

    PZ --> PM --> WG
    PZ --> DR --> VT --> XM
    NR --> XM
    PRN --> XM
    BL --> DR
    BL --> SC
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

| Work              | Why it is not a part                                                                                                                                                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| OpenBot mode      | Drawn as OB, a context part. toolboxmd/openbot#142 only records the direction, so it cannot be the primary ref: the tab would colour the mode done. Chromeria spike #7 closed as not planned, and no open issue owns the mode here. |
| v2 cutover (#177) | The landing umbrella. Promachos, Wight, Drafters, Spectrum and Prism cite their own issues.                                                                                                                                         |
