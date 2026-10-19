import { type Color } from "@t3tools/contracts";
import {
  closestCenter,
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { restrictToVerticalAxis } from "@dnd-kit/modifiers";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useState } from "react";
import { ArrowDownIcon, ArrowUpIcon, GripVerticalIcon, TrashIcon } from "lucide-react";
import { cn } from "../../lib/utils";
import { Button } from "../ui/button";
import {
  movePrismPreference,
  nextPrismEntryId,
  syncPrismDraft,
  type PrismDraftPreference,
} from "./PrismSettings.logic";
import { ColorPicker } from "./ColorPicker";
import { defaultColor, findColorChoice, type ColorProviderChoice } from "./ColorPicker.logic";

type DraftPreference = PrismDraftPreference;
const editablePreference = (model: Color, entryId: number): DraftPreference => ({
  ...model,
  entryId,
});
const savedPreferences = (models: readonly DraftPreference[]) =>
  models.map(({ entryId: _entryId, ...model }) => model);

function ModelRow({
  entry,
  index,
  count,
  title,
  providers,
  disabled,
  onChange,
  onMove,
  onRemove,
}: {
  entry: DraftPreference;
  index: number;
  count: number;
  title: string;
  providers: readonly ColorProviderChoice[];
  disabled: boolean;
  onChange: (color: Color) => void;
  onMove: (direction: -1 | 1) => void;
  onRemove: () => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: entry.entryId,
    disabled,
  });
  const available = findColorChoice(providers, entry).model !== undefined;
  return (
    <li
      ref={setNodeRef}
      style={{ transform: CSS.Transform.toString(transform), transition }}
      className={cn(
        "flex flex-wrap items-center gap-2 rounded-lg border bg-card p-2",
        isDragging && "relative z-10 shadow-md",
      )}
    >
      <span className="touch-none">
        <Button
          size="icon-sm"
          variant="ghost-muted"
          aria-label={`Drag ${title} preference ${index + 1}`}
          disabled={disabled}
          {...attributes}
          {...listeners}
        >
          <GripVerticalIcon />
        </Button>
      </span>
      <span className="w-16 shrink-0 text-xs text-muted-foreground">
        {index === 0 ? "Primary" : `Fallback ${index}`}
      </span>
      <span className="min-w-0 flex-1">
        <ColorPicker
          value={entry}
          providers={providers}
          label={`${title} preference ${index + 1}`}
          disabled={disabled}
          onChange={onChange}
        />
        {!available && (
          <span className="mt-1 block text-xs text-muted-foreground">
            Unavailable on one or more selected targets.
          </span>
        )}
      </span>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={`Move ${title} preference ${index + 1} up`}
        disabled={disabled || index === 0}
        onClick={() => onMove(-1)}
      >
        <ArrowUpIcon />
      </Button>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={`Move ${title} preference ${index + 1} down`}
        disabled={disabled || index === count - 1}
        onClick={() => onMove(1)}
      >
        <ArrowDownIcon />
      </Button>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={`Remove ${title} preference ${index + 1}`}
        disabled={disabled}
        onClick={onRemove}
      >
        <TrashIcon />
      </Button>
    </li>
  );
}

/** One ordered list, saved on every change like the Providers page. */
export function ColorList({
  title,
  saved,
  providers,
  mixed,
  disabled,
  saving,
  save,
}: {
  title: string;
  saved: readonly Color[];
  providers: readonly ColorProviderChoice[];
  mixed: boolean;
  disabled: boolean;
  saving: boolean;
  save: (models: readonly Color[]) => Promise<void>;
}) {
  const [draft, setDraft] = useState<readonly DraftPreference[]>(() =>
    saved.map(editablePreference),
  );
  // The draft leads while a write is in flight; afterwards it follows what the server streams.
  if (!saving) {
    const synced = syncPrismDraft(draft, saved);
    if (synced !== draft) setDraft(synced);
  }
  // The Color being added; null follows the scope's first choice.
  const [adding, setAdding] = useState<Color | null>(null);
  const addColor = adding ?? defaultColor(providers);
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 4 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  );
  const commit = (next: readonly DraftPreference[]) => {
    // One write at a time: an edit while one is in flight is ignored, so this save owns the restore.
    if (saving) return;
    setDraft(next);
    // The page shows the failure; the list returns to what is saved.
    save(savedPreferences(next)).catch(() => setDraft((current) => syncPrismDraft(current, saved)));
  };
  const onDragEnd = ({ active, over }: DragEndEvent) => {
    const from = draft.findIndex((entry) => entry.entryId === active.id);
    const to = draft.findIndex((entry) => entry.entryId === over?.id);
    if (from >= 0 && to >= 0 && from !== to) commit(arrayMove([...draft], from, to));
  };
  return (
    <div className="space-y-3">
      {mixed && (
        <p className="text-sm text-muted-foreground">
          Preferences differ across this scope. A change replaces this list on the selected targets.
        </p>
      )}
      {draft.length === 0 && <p className="text-sm text-muted-foreground">No Colors configured.</p>}
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        modifiers={[restrictToVerticalAxis]}
        onDragEnd={onDragEnd}
      >
        <SortableContext
          items={draft.map((entry) => entry.entryId)}
          strategy={verticalListSortingStrategy}
        >
          <ol className="space-y-2">
            {draft.map((entry, index) => (
              <ModelRow
                key={entry.entryId}
                entry={entry}
                index={index}
                count={draft.length}
                title={title}
                providers={providers}
                disabled={disabled}
                onChange={(color) =>
                  commit(
                    draft.map((model, position) =>
                      position === index ? { ...color, entryId: model.entryId } : model,
                    ),
                  )
                }
                onMove={(direction) => commit(movePrismPreference(draft, index, direction))}
                onRemove={() => commit(draft.filter((_, position) => position !== index))}
              />
            ))}
          </ol>
        </SortableContext>
      </DndContext>
      {addColor && (
        <div className="flex flex-wrap items-center gap-2">
          <ColorPicker
            value={addColor}
            providers={providers}
            label={`New ${title} Color`}
            disabled={disabled}
            onChange={setAdding}
          />
          <Button
            size="sm"
            variant="outline"
            disabled={disabled || saving || !findColorChoice(providers, addColor).model}
            onClick={() => {
              commit([...draft, editablePreference(addColor, nextPrismEntryId(draft))]);
              setAdding(null);
            }}
            aria-label={`Add ${title} Color`}
          >
            Add Color
          </Button>
        </div>
      )}
      {providers.length === 0 && (
        <p className="text-xs text-muted-foreground">
          Enable models in Providers for this scope to add preferences.
        </p>
      )}
    </div>
  );
}
