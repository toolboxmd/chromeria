# Chromeria map

The parts of Chromeria and how they connect. The Map tab colours each part from
GitHub: planned, working, broken or done. Live parts are existing capabilities
asserted by this map; the crystal in the part list marks Chromeria-tracked work.

Each part is declared once as `ID["Name"]`. The comment line `%% ID: refs` under it
names the part's Issues and pull requests; the first ref is the primary ref, which
decides planned, working or done. `%% ID: live` marks an existing capability in this
project; `%% ID: context` marks a part another project owns. Live, context and refs
are mutually exclusive. GitHub ignores these comment lines.

Change this map in the pull request that adds, splits, merges or removes a part.

```mermaid
flowchart TD
    subgraph T3["Live T3 base"]
        T3C["Clients and settings"]
        %% T3C: live
        T3S["Server and orchestration"]
        %% T3S: live
        T3P["Provider adapters"]
        %% T3P: live
        T3T["Projects, threads, turns and checkpoints"]
        %% T3T: live
        T3R["Remote environments and T3 Connect"]
        %% T3R: live
        T3M["MCP tools and scheduled tasks"]
        %% T3M: live
        T3C --> T3S --> T3P
        T3S --> T3T
        T3S --> T3R
        T3S --> T3M
    end

    PZ["Prism routing"]
    %% PZ: #92 PR #408 PR #430 PR #479 PR #489 PR #512
    VT["Visible T3 threads"]
    %% VT: #10
    PM["Promachos bubbles and persona"]
    %% PM: #116 #400 PR #499
    WG["Wight mode"]
    %% WG: #117
    BL["Bare launch and receipts"]
    %% BL: #289 PR #335
    DR["Drafters in any project"]
    %% DR: #114 #429 PR #475
    SC["Spectrum council and free"]
    %% SC: #118 PR #494 PR #509
    JT["Spectrum judged tests"]
    %% JT: #119
    OB["OpenBot: Bots, Channels, Computer"]
    %% OB: context
    UA["Upstream absorption"]
    %% UA: #411 #166
    MI["Mobile identity"]
    %% MI: #205
    BR["Bounded event reads"]
    %% BR: #256
    TIW["Turn-item writes"]
    %% TIW: #425 PR #433
    RO["Run observers"]
    %% RO: #456 PR #457
    PU["Provider updates"]
    %% PU: #263
    RS["Resume after restart"]
    %% RS: #264 #255 PR #417
    SN["Child notices"]
    %% SN: #269 #341 #422 PR #431
    ST["Urgent interruption"]
    %% ST: #285 #298 PR #478
    CK["Command keys"]
    %% CK: #232
    SP["Shown proof"]
    %% SP: #245
    TF["Test Server 6 flake"]
    %% TF: #259
    XM["Cross-machine control"]
    %% XM: #164 #302
    NR["Network routes"]
    %% NR: #292 #358
    PRN["Paired session renewal"]
    %% PRN: #323 #336
    WR["Agent web research"]
    %% WR: #423 PR #453
    WRR["Headless page rendering"]
    %% WRR: #442
    WR --- WRR

    T3P --- PZ
    T3T --- VT
    T3C --- PM
    T3S --- WG
    T3P --- BL
    T3M --- DR
    T3M --- SC
    T3M --- JT
    T3S --- UA
    T3C --- MI
    T3S --- BR
    T3S --- TIW
    T3S --- RO
    T3P --- PU
    T3T --- RS
    T3T --- SN
    T3T --- ST
    T3C --- CK
    T3S --- SP
    T3S --- TF
    T3R --- XM
    T3R --- NR
    T3R --- PRN
    T3M --- WR

    PZ --> PM --> WG
    PZ --> DR --> VT --> XM
    NR --> XM
    PRN --> XM
    BL --> DR
    BL --> SC
    DR --> SC --> JT
    DR --> SN
    VT --> RS
    VT --> ST
    OB -.-> PM
    UA --> PZ
    MI --> UA
    CK --- SP
    TF -.-> SN
    BR --- PU
```

## Work that is not a part

| Work              | Why it is not a part                                                                                                                                                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| OpenBot mode      | Drawn as OB, a context part. toolboxmd/openbot#142 only records the direction, so it cannot be the primary ref: the tab would colour the mode done. Chromeria spike #7 closed as not planned, and no open issue owns the mode here. |
| v2 cutover (#177) | The landing umbrella. Promachos, Wight, Drafters, Spectrum and Prism cite their own issues.                                                                                                                                         |
