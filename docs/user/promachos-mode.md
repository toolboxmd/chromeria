# Promachos mode

Promachos mode shows your conversations with the Promachos as a messenger. His
replies show as bubbles, one per paragraph, and yours on the right. While he works,
one status line under your message names what he is doing, then turns into
"Typing…" just before his reply. Tables sit on their own card, and things that
happened without a message, such as a Drafter he started, show as one gray line.
Questions and approvals appear as cards in the chat.

## Start

1. On web or desktop, choose **Promachos** in the switch at the top of the sidebar.
2. Choose the project that is the Promachos home, or choose **New home** to create
   one. The choice belongs to that project on that environment, and each browser or
   desktop app remembers its own.
3. Use the round **+** button to start a conversation.

Choose **Code** in the same switch to return to the standard view. To pick another
home, open the round home button. The conversation's actions, such as rename,
files and git, are in the menu on its title and the round **…** button next to it.

## On the phone

The mobile app has a **Promachos** tab in the bottom bar. It lists the home's
conversations, and the round **+** starts one. Chats look and behave as on desktop;
files, git and terminal are in the round **…** menu. The phone remembers its own
home: choose it in the tab, and tap the round home button to choose another.

Opening the app without a link or notification returns to the tab you used last. On
the Promachos tab that is the conversation you had open, or the list if you had left
it.

## Create a home

**New home** asks for a folder, `~/promachos` unless you change it, and creates it on
the environment's own machine, so it works for remote environments too. The folder
becomes a git repository with a starter `AGENTS.md` and a `CLAUDE.md` that points to
it, is added as a project, and is selected as the home.

Write the Promachos persona under "Who you are" in that `AGENTS.md`. Every
conversation in the home reads it.

Files, a git repository or a project already there are kept as they are, so you can
point **New home** at an existing folder. If creation fails, the picker shows why;
fix the cause and choose **Create** again.

## Good to know

- Only conversations in the home show as chats. Other threads keep the standard view,
  and agents a conversation starts stay in its Lineage section.
- Type a custom answer to a question in the composer, as in the standard view.
- The Promachos can answer another thread's question or approval for you when no
  parent thread can: the thread has no parent, or its parent was deleted or has
  been idle for 30 minutes. A parent that is still active answers first.
  The answered request shows "Answered by the Promachos for you". This works only
  in conversations started in Promachos mode after this update, while the
  Promachos list has a model.
- A new conversation starts on the first model with capacity in the Promachos list
  under **Settings → Promachos**. If that list is empty, it starts on the model picked in
  the composer. If no listed model has capacity, the conversation does not start and
  the chat shows why.
- The right panel and the composer work as they do in the standard view.
