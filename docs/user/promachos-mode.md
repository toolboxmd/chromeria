# Promachos mode

Promachos mode is a chat-first view of your conversations with the Promachos. His
replies show as bubbles, one per paragraph, and his tool activity collapses into a
working indicator. Questions and approvals appear as cards in the chat.

## Start

1. On web or desktop, choose **Promachos** in the switch at the top of the sidebar.
2. Choose the project that is the Promachos home, or choose **New home** to create
   one. The choice belongs to that project on that environment, and each browser or
   desktop app remembers its own.
3. Use the new conversation button next to the home's name to start a conversation.

Choose **Code** in the same switch to return to the standard view. To pick another
home, open the menu on the home's name.

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
  and agents a conversation starts stay in its Agents panel.
- Type a custom answer to a question in the composer, as in the standard view.
- A new conversation starts on the first model with capacity in the Promachos list
  under **Settings → Prism**. If that list is empty, it starts on the model picked in
  the composer. If no listed model has capacity, the conversation does not start and
  the chat shows why.
- The right panel and the composer work as they do in the standard view.
