# Court Security Scheduling App
## Court Deputy User Manual (Non-Technical)

> **Purpose:** This guide explains the daily workflow for court deputies and supervisors using the Court Security Scheduling app.
>
> **PDF-Ready:** Copy this document into Microsoft Word, apply your preferred department header/logo, then **Save As → PDF**.

---

## 1) Getting Started

1. Open the app and sign in.
2. Use the top navigation buttons:
   - **Search Page** = main assignment page.
   - **Roster Page** = add/edit people and set leave/unavailable statuses.
   - **Staff Resources Daily Page** = staffing reference counts.
3. Select the correct **Date** before making any changes.

---

## 2) Roster Page (People + Availability)

The **Roster Page** is where you maintain who is in the system and who is available.

### What you see
- A **Date** field at the top.
- **Add Person** area (name, division, rank, capability).
- Two tables:
  - **Court Security**
  - **Other Divisions**
- Status dropdowns for each person (Available, Scheduled Leave, Unscheduled Leave, Unavailable, Training).

### How to add people to roster
1. Go to **Roster Page**.
2. In **Add Person**, enter name as: `Last name, First name`.
3. Choose **Division**, **Rank**, and **Capability**.
4. Click **Add Person**.
5. Confirm success message appears.

> Tip: If name format is wrong, the app will reject the entry.

### How to mark leave/unavailability
1. In the deputy row, use the **Status** dropdown.
2. If choosing a non-available status, set:
   - **Start date**
   - **End date**
3. Click **Apply**.

What happens automatically:
- The app removes overlapping status ranges first.
- The new range is saved.
- If needed, the app can remove the deputy from assignments that conflict with the leave range.

### Editing an existing person
- Change division/rank/capability in-row.
- Click **Save** in that row.

---

## 3) Assignment Page (Daily Court Assignments)

The **Search Page** is the primary assignment board.

### Main sections
- **Fixed Posts**
- **Courtrooms**
- **Overtime**
- Right panel: **Court Security Roster** (deputies list)

### How to assign deputies
You can assign deputies in two easy ways:

#### Option A (easiest): Drag and drop
1. Choose the date.
2. Find deputy in the roster list (right panel).
3. Drag deputy onto an assignment cell.
4. Repeat as needed.

#### Option B: Click-to-assign
1. Click a deputy name in the roster list (it becomes selected).
2. Click an assignment cell.
3. Deputy is placed into that assignment.

### Important assignment notes
- Some assignment cells allow multiple names (chips).
- If a deputy is already assigned elsewhere, permissions decide whether duplicate posting is allowed.
- Assignment changes are staged first, then committed when you click **Save Assignments**.

---

## 4) Overtime Workflow

Overtime is handled in the **Overtime** table and selected OT fixed-post rows.

### How to assign overtime
1. Go to the date on **Search Page**.
2. In the **Overtime** section, assign a deputy to the OT row.
3. Confirm **Shift** time is set.
   - If shift time is missing, app prompts for time entry.
4. Add optional notes in the OT notes field.
5. Click **Save Assignments**.

### OT hours display
- The header shows an **OT Hours** total badge.
- Total reflects only OT rows that have both:
  - a deputy assigned, and
  - a valid shift range.

---

## 5) Transfers (Move deputy from one post to another)

Use transfers when a deputy leaves one post and reports to another.

### Transfer by dragging between assignments
1. Drag deputy chip from one assignment cell to another.
2. Transfer time popup appears.
3. Enter transfer-out time and save.
4. App marks old location as transferred-out and places deputy in new location with transfer-in tracking.

### Transfer from action menu
1. Click deputy chip in an assignment cell.
2. Choose **Transfer Out**.
3. Enter transfer time.
4. Save.

### Transfer limits and cleanup
- Maximum of **3 transfers per deputy per day**.
- You can use **Remove transfer data** from chip menu if needed.

---

## 6) Saving Behavior (Very Important)

The app uses **staged edits**:
- Drag/drop, notes edits, judge edits, and shift edits are queued locally first.
- Nothing is final until you click **Save Assignments**.

### How to save
1. Make all assignment edits for the day.
2. Click **Save Assignments**.
3. Watch status text:
   - **Saving...**
   - **Saved** (success)
   - **Save failed** (retry needed)

### What gets saved when you click Save Assignments
- Deputy assignments
- Assignment notes
- Judge names
- Courtroom metadata
- Overtime/fixed-post shift times

---

## 7) Importing From Previous Day

There are two import workflows in this app.

### A) Assignment page import (previous weekday)
Use this to pre-fill daily assignments from the prior weekday.

1. Open **Search Page**.
2. Set target date.
3. Click **Import**.
4. App copies assignments from previous weekday into open slots.
5. App skips deputies marked unavailable for the target date.
6. Review all assignments and click **Save Assignments**.

### B) Staffing daily column import
Used by staffing table logic (separate from main assignment board).
- The backend supports importing the most recent previous column data into selected date.

---

## 8) Quick Daily Checklist (Recommended)

1. Set correct **Date**.
2. On **Roster Page**, verify availability/leave statuses.
3. On **Search Page**, assign fixed posts and courtrooms.
4. Fill overtime and shift times.
5. Record transfers as deputies move.
6. Click **Save Assignments**.
7. Confirm **Saved** appears.
8. Optional: reopen date to verify final layout.

---

## 9) Common Mistakes to Avoid

- Forgetting to click **Save Assignments** after making changes.
- Entering a new person with wrong name format.
- Importing before selecting the correct date.
- Missing OT shift time after assigning OT deputy.
- Ignoring transfer prompts when moving someone between posts.

---

## 10) One-Page Quick Reference

- **Assign deputy:** drag from roster to assignment cell.
- **Use overtime:** assign in OT row + set shift time.
- **Save:** click **Save Assignments** and wait for **Saved**.
- **Transfer:** click chip → Transfer Out, or drag between posts.
- **Add roster person:** Roster Page → Add Person.
- **Import previous day:** Search Page → date → Import.

---

**End of Manual**
