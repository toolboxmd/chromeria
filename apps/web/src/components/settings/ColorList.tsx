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
import { T, useGT } from "gt-react";
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
import { ColorPicker, ProviderLabel } from "./ColorPicker";
import { findColorChoice, type ColorProviderChoice } from "./ColorPicker.logic";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";

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
  minItems,
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
  minItems: number;
}) {
  const gt = useGT();
  const position = index + 1;
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
          aria-label={gt("Drag {title} preference {position}", { title, position })}
          disabled={disabled}
          {...attributes}
          {...listeners}
        >
          <GripVerticalIcon />
        </Button>
      </span>
      <span className="w-16 shrink-0 text-xs text-muted-foreground">
        {index === 0
          ? gt("Primary", { $context: "the first model in a fallback list" })
          : gt("Fallback {index}", { index })}
      </span>
      <span className="min-w-0 flex-1">
        <ColorPicker
          value={entry}
          providers={providers}
          label={gt("{title} preference {position}", { title, position })}
          disabled={disabled}
          onChange={onChange}
        />
        {!available && (
          <span className="mt-1 block text-xs text-muted-foreground">
            <T>Unavailable on one or more selected targets.</T>
          </span>
        )}
      </span>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={gt("Move {title} preference {position} up", { title, position })}
        disabled={disabled || index === 0}
        onClick={() => onMove(-1)}
      >
        <ArrowUpIcon />
      </Button>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={gt("Move {title} preference {position} down", { title, position })}
        disabled={disabled || index === count - 1}
        onClick={() => onMove(1)}
      >
        <ArrowDownIcon />
      </Button>
      <Button
        size="icon-sm"
        variant="ghost"
        aria-label={gt("Remove {title} preference {position}", { title, position })}
        disabled={disabled || count <= minItems}
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
  minItems = 0,
  maxItems = Infinity,
}: {
  title: string;
  saved: readonly Color[];
  providers: readonly ColorProviderChoice[];
  mixed: boolean;
  disabled: boolean;
  saving: boolean;
  save: (models: readonly Color[]) => Promise<void>;
  minItems?: number;
  maxItems?: number;
}) {
  const gt = useGT();
  const [draft, setDraft] = useState<readonly DraftPreference[]>(() =>
    saved.map(editablePreference),
  );
  // The draft leads while a write is in flight; afterwards it follows what the server streams.
  if (!saving) {
    const synced = syncPrismDraft(draft, saved);
    if (synced !== draft) setDraft(synced);
  }
  const addKind = draft.length === 0 ? "primary" : "fallback";
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
          <T>
            Preferences differ across this scope. A change replaces this list on the selected
            targets.
          </T>
        </p>
      )}
      {draft.length === 0 && (
        <p className="text-sm text-muted-foreground">
          <T>No primary model configured.</T>
        </p>
      )}
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
                minItems={minItems}
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
      {providers.length > 0 && draft.length < maxItems && (
        // Picking a provider saves its first model at once; the new row then edits model and effort.
        <Select
          value={null}
          disabled={disabled || saving}
          onValueChange={(instanceId) => {
            const provider = providers.find((candidate) => candidate.instanceId === instanceId);
            const model = provider?.models[0];
            if (!provider || !model) return;
            const color = { instanceId: provider.instanceId, model: model.slug };
            commit([...draft, editablePreference(color, nextPrismEntryId(draft))]);
          }}
        >
          <SelectTrigger
            size="sm"
            className="w-fit"
            aria-label={
              addKind === "primary"
                ? gt("Add {title} primary", { title })
                : gt("Add {title} fallback", { title })
            }
          >
            <SelectValue>
              {addKind === "primary" ? gt("Add primary") : gt("Add fallback")}
            </SelectValue>
          </SelectTrigger>
          <SelectPopup>
            {providers.map((candidate) => (
              <SelectItem key={candidate.instanceId} value={candidate.instanceId}>
                <ProviderLabel provider={candidate} />
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      )}
      {providers.length === 0 && (
        <p className="text-xs text-muted-foreground">
          <T>Enable models in Providers for this scope to add preferences.</T>
        </p>
      )}
    </div>
  );
}
