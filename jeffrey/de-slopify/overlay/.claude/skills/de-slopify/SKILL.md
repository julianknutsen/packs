---
name: de-slopify
description: >-
  Remove telltale AI writing patterns from documentation and text.
  Direct, concise prose without AI artifacts.
---

# The De-Slopifier

Read through the complete text carefully and look for any telltale signs
of "AI slop" style writing.

## What to Remove

**Em dashes:** One of the biggest tells. Replace with a semicolon, a
comma, or recast the sentence so it sounds natural while avoiding em
dashes entirely.

**AI-isms:** Sentences of the form "It's not [just] XYZ, it's ABC" or
"Here's why" or "Here's why it matters:" or "Let's dive in." Anything
that sounds like the kind of thing an LLM would write disproportionately
more commonly than a human writer and which sounds
inauthentic or cringe.

**Filler openers:** "Certainly!", "Great question!", "Absolutely!",
"I'd be happy to help!", "Sure thing!" Remove these entirely.

**Hedging:** "I think maybe", "It might be possible that", "It's worth
noting that." Be direct instead.

**Excessive bullets:** Where flowing prose would work better. Not every
list needs bullet points.

**Gratuitous emoji:** Unless the original text intentionally uses emoji
for a specific purpose.

**Meta-commentary:** "As an AI", "In my experience as a language model",
"I was trained to." Remove all self-referential AI commentary.

**Redundant transitions:** "Furthermore", "Moreover", "Additionally" used
as paragraph openers when the connection is already clear.

## Method

You cannot do this using regex or a script. You MUST manually read each
line of the text and revise it manually in a systematic, methodical,
diligent way.

Read the output aloud (mentally) to catch unnatural phrasing.

## Target

Apply to the specified file, selection, or most recent output. If no
target is specified, ask what text to de-slopify.

---

*Based on Jeffrey Emanuel's De-Slopifier (@doodlestein)*
