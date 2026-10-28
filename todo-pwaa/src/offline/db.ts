import {
  openDB,
  type DBSchema,
  type IDBPDatabase,
} from "idb";

import type { Task, TaskPatch } from "../types";
import { isLocalId } from "../utils/tasks.ts";

// ---------- Operaciones pendientes ----------

export type OutboxOp =
  | {
      id: string;
      op: "create";
      clienteId: string;
      data: Task;
      ts: number;
      rev: number;
    }
  | {
      id: string;
      op: "update";
      serverId: string;
      data: TaskPatch;
      ts: number;
      rev: number;
    }
  | {
      id: string;
      op: "delete";
      // Puede ser un ID local hasta que exista mapping.
      serverId: string;
      ts: number;
      rev: number;
    };

interface TodoDB extends DBSchema {
  tasks: {
    key: string;
    value: Task;
  };

  outbox: {
    key: string;
    value: OutboxOp;
  };

  meta: {
    key: string;
    value: {
      key: string;
      value: string;
    };
  };
}

let dbp: Promise<IDBPDatabase<TodoDB>> | undefined;

export function db(): Promise<IDBPDatabase<TodoDB>> {
  if (!dbp) {
    dbp = openDB<TodoDB>("todo-pwa", 2, {
      upgrade(database) {
        if (!database.objectStoreNames.contains("tasks")) {
          database.createObjectStore("tasks", {
            keyPath: "_id",
          });
        }

        if (!database.objectStoreNames.contains("outbox")) {
          database.createObjectStore("outbox", {
            keyPath: "id",
          });
        }

        if (!database.objectStoreNames.contains("meta")) {
          database.createObjectStore("meta", {
            keyPath: "key",
          });
        }
      },
    }).catch((error: unknown) => {
      dbp = undefined;
      throw error;
    });
  }

  return dbp;
}

function sortOperations(ops: OutboxOp[]): OutboxOp[] {
  return [...ops].sort(
    (a, b) => a.ts - b.ts || a.id.localeCompare(b.id)
  );
}

// ---------- Caché ----------

// Combina la respuesta del servidor con operaciones pendientes.
// No elimina las tareas offline al refrescar desde la API.
export async function cacheTasks(list: Task[]): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const tasks = tx.objectStore("tasks");
  const outbox = tx.objectStore("outbox");
  const meta = tx.objectStore("meta");

  const ops = sortOperations(await outbox.getAll());
  const records = await meta.getAll();

  const mappings = new Map<string, string>();

  for (const record of records) {
    if (record.key.startsWith("map:")) {
      mappings.set(record.key.slice(4), record.value);
    }
  }

  const merged = applyOutbox(list, ops, mappings);

  await tasks.clear();

  for (const task of merged) {
    await tasks.put(task);
  }

  await tx.done;
}

export async function getCachedTasks(): Promise<Task[]> {
  return (await db()).getAll("tasks");
}

export async function getAllTasksLocal(): Promise<Task[]> {
  return getCachedTasks();
}

export async function putTaskLocal(task: Task): Promise<void> {
  await (await db()).put("tasks", task);
}

export async function removeTaskLocal(id: string): Promise<void> {
  await (await db()).delete("tasks", id);
}

// ---------- Mapping ----------

export async function setMapping(
  clienteId: string,
  serverId: string
): Promise<void> {
  await (await db()).put("meta", {
    key: `map:${clienteId}`,
    value: serverId,
  });
}

export async function getMapping(
  clienteId: string
): Promise<string | undefined> {
  const record = await (await db()).get(
    "meta",
    `map:${clienteId}`
  );

  return record?.value;
}

// Guarda el mapping y cambia el ID en una transacción.
// No confirma operaciones: eso corresponde al sync.
export async function promoteLocalToServer(
  clienteId: string,
  serverId: string
): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const tasks = tx.objectStore("tasks");
  const outbox = tx.objectStore("outbox");

  await tx.objectStore("meta").put({
    key: `map:${clienteId}`,
    value: serverId,
  });

  const local = await tasks.get(clienteId);
  const existing = await tasks.get(serverId);

  const localDelete = await outbox.get(
    `delete:${clienteId}`
  );

  const serverDelete = await outbox.get(
    `delete:${serverId}`
  );

  if (localDelete || serverDelete) {
    await tasks.delete(clienteId);
    await tasks.delete(serverId);
  } else if (local || existing) {
    await tasks.put({
      ...(existing ?? local!),
      _id: serverId,
      clienteId,
      pending: true,
    });

    if (clienteId !== serverId) {
      await tasks.delete(clienteId);
    }
  }

  await tx.done;
}

// ---------- Outbox ----------

export async function getOutbox(): Promise<OutboxOp[]> {
  return sortOperations(
    await (await db()).getAll("outbox")
  );
}

// Guarda tarea y creación juntas.
export async function queueCreate(task: Task): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox"],
    "readwrite"
  );

  const outbox = tx.objectStore("outbox");
  const key = `create:${task._id}`;

  const previous = await outbox.get(key);
  const deletion = await outbox.get(`delete:${task._id}`);

  if (deletion) {
    await tx.done;
    throw new Error(
      `La tarea ${task._id} tiene una eliminación pendiente`
    );
  }

  const local: Task = {
    ...task,
    clienteId: task._id,
    pending: true,
  };

  await tx.objectStore("tasks").put(local);

  await outbox.put({
    id: key,
    op: "create",
    clienteId: task._id,
    data: local,
    ts: previous?.ts ?? Date.now(),
    rev: (previous?.rev ?? 0) + 1,
  });

  await tx.done;
}

// Guarda tarea y actualización juntas.
export async function queueUpdate(
  id: string,
  patch: TaskPatch
): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const tasks = tx.objectStore("tasks");
  const outbox = tx.objectStore("outbox");

  const mapping = await tx
    .objectStore("meta")
    .get(`map:${id}`);

  const targetId = mapping?.value ?? id;

  const originalDelete = await outbox.get(`delete:${id}`);
  const targetDelete = await outbox.get(
    `delete:${targetId}`
  );

  if (originalDelete || targetDelete) {
    await tx.done;
    throw new Error(
      `La tarea ${id} tiene una eliminación pendiente`
    );
  }

  const create = await outbox.get(`create:${id}`);

  const targetTask = await tasks.get(targetId);
  const originalTask =
    targetId === id ? targetTask : await tasks.get(id);

  const local =
    targetTask ??
    originalTask ??
    (create?.op === "create" ? create.data : undefined);

  if (!local) {
    await tx.done;
    throw new Error(`No existe la tarea local ${id}`);
  }

  const nextTask: Task = {
    ...local,
    ...patch,
    _id: targetId,
    pending: true,
  };

  if (!mapping && isLocalId(id)) {
    if (create?.op !== "create") {
      await tx.done;
      throw new Error(
        `No existe creación pendiente ni mapping para ${id}`
      );
    }

    await outbox.put({
      ...create,
      data: {
        ...create.data,
        ...patch,
        _id: id,
        pending: true,
      },
      rev: create.rev + 1,
    });
  } else {
    const key = `update:${targetId}`;
    const previous = await outbox.get(key);

    await outbox.put({
      id: key,
      op: "update",
      serverId: targetId,
      data: {
        ...(previous?.op === "update"
          ? previous.data
          : {}),
        ...patch,
      },
      ts: previous?.ts ?? Date.now(),
      rev: (previous?.rev ?? 0) + 1,
    });
  }

  await tasks.put(nextTask);

  if (targetId !== id) {
    await tasks.delete(id);
  }

  await tx.done;
}

// Mantiene el create pendiente: puede estar siendo enviado.
// El sync debe resolver esa creación y después eliminarla.
export async function queueDelete(id: string): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const tasks = tx.objectStore("tasks");
  const outbox = tx.objectStore("outbox");

  const mapping = await tx
    .objectStore("meta")
    .get(`map:${id}`);

  const targetId = mapping?.value ?? id;

  const key = `delete:${targetId}`;
  const previous = await outbox.get(key);

  await outbox.delete(`update:${id}`);

  if (targetId !== id) {
    await outbox.delete(`update:${targetId}`);
  }

  await outbox.put({
    id: key,
    op: "delete",
    serverId: targetId,
    ts: previous?.ts ?? Date.now(),
    rev: (previous?.rev ?? 0) + 1,
  });

  await tasks.delete(id);

  if (targetId !== id) {
    await tasks.delete(targetId);
  }

  await tx.done;
}

// Confirma únicamente la revisión que se envió.
export async function removeOp(op: OutboxOp): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    "outbox",
    "readwrite"
  );

  const current = await tx.store.get(op.id);

  if (current && current.rev === op.rev) {
    await tx.store.delete(op.id);
  }

  await tx.done;
}

// Compatibilidad con otros imports.
// No utilizar para confirmar operaciones del sync.
export async function removeOutbox(id: string): Promise<void> {
  await (await db()).delete("outbox", id);
}

// Confirma revisiones y recalcula pending en una transacción.
// Si llegó otra revisión, permanece pendiente.
// El sync debe promover primero los IDs locales.
export async function acknowledgeOperations(
  ops: OutboxOp[]
): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const tasks = tx.objectStore("tasks");
  const outbox = tx.objectStore("outbox");
  const meta = tx.objectStore("meta");

  for (const op of ops) {
    const current = await outbox.get(op.id);

    if (current?.rev === op.rev) {
      await outbox.delete(op.id);
    }
  }

  const mappings = new Map<string, string>();

  for (const record of await meta.getAll()) {
    if (record.key.startsWith("map:")) {
      mappings.set(record.key.slice(4), record.value);
    }
  }

  const resolve = (id: string) => mappings.get(id) ?? id;

  const affected = new Set(
    ops.map((op) =>
      resolve(
        op.op === "create"
          ? op.clienteId
          : op.serverId
      )
    )
  );

  const pendingIds = new Set(
    (await outbox.getAll()).map((op) =>
      resolve(
        op.op === "create"
          ? op.clienteId
          : op.serverId
      )
    )
  );

  for (const task of await tasks.getAll()) {
    const id = resolve(task._id);

    if (affected.has(id)) {
      await tasks.put({
        ...task,
        pending: pendingIds.has(id),
      });
    }
  }

  await tx.done;
}

// ---------- Vista del servidor + cambios pendientes ----------

export function applyOutbox(
  server: Task[],
  ops: OutboxOp[],
  mappings: ReadonlyMap<string, string> = new Map()
): Task[] {
  const identities = new Map(mappings);

  for (const task of server) {
    if (task.clienteId) {
      identities.set(task.clienteId, task._id);
    }
  }

  const resolve = (id: string) =>
    identities.get(id) ?? id;

  const byId = new Map<string, Task>(
    server.map((task) => [
      task._id,
      { ...task, pending: false },
    ])
  );

  const ordered = sortOperations(ops);

  // Primero aplicar creaciones y actualizaciones.
  for (const op of ordered) {
    if (op.op === "create") {
      const id = resolve(op.clienteId);
      const existing = byId.get(id);

      byId.set(id, {
        ...existing,
        ...op.data,
        _id: id,
        clienteId: op.clienteId,
        pending: true,
      });
    } else if (op.op === "update") {
      const id = resolve(op.serverId);
      const task = byId.get(id);

      if (task) {
        byId.set(id, {
          ...task,
          ...op.data,
          _id: id,
          pending: true,
        });
      }
    }
  }

  // Una eliminación pendiente siempre oculta la tarea.
  for (const op of ordered) {
    if (op.op === "delete") {
      byId.delete(resolve(op.serverId));
      byId.delete(op.serverId);
    }
  }

  return [...byId.values()];
}

// ---------- Sesión ----------

export async function clearLocalData(): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  await tx.objectStore("tasks").clear();
  await tx.objectStore("outbox").clear();
  await tx.objectStore("meta").clear();

  await tx.done;
}

export async function ensureLocalOwner(
  userId: string
): Promise<void> {
  const database = await db();

  const tx = database.transaction(
    ["tasks", "outbox", "meta"],
    "readwrite"
  );

  const meta = tx.objectStore("meta");
  const owner = await meta.get("owner");

  if (owner && owner.value !== userId) {
    await tx.objectStore("tasks").clear();
    await tx.objectStore("outbox").clear();
    await meta.clear();
  }

  await meta.put({
    key: "owner",
    value: userId,
  });

  await tx.done;
}