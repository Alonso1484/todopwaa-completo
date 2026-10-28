import { api } from "../api";

import {
  getOutbox,
  getMapping,
  promoteLocalToServer,
  acknowledgeOperations,
  type OutboxOp,
} from "./db";

import type { Task, TaskPatch } from "../types";

interface SyncMapping {
  clienteId: string;
  serverId: string;
}

interface BulkSyncResponse {
  mapping?: SyncMapping[];
  skipped?: number;
}

let syncing: Promise<void> | null = null;
let requested = false;

function operationTaskId(op: OutboxOp): string {
  return op.op === "create"
    ? op.clienteId
    : op.serverId;
}

// El backend mostrado utiliza ObjectId de MongoDB.
function isServerId(id: string): boolean {
  return /^[a-f\d]{24}$/i.test(id);
}

async function resolveServerId(
  id: string
): Promise<string | undefined> {
  const mapped = await getMapping(id);

  if (mapped) return mapped;

  return isServerId(id) ? id : undefined;
}

function editableFields(task: Task): TaskPatch {
  return {
    title: task.title,
    description: task.description,
    status: task.status,
    priority: task.priority,
    dueDate: task.dueDate,
  };
}

function getHttpStatus(error: unknown): number | undefined {
  return (
    error as {
      response?: {
        status?: number;
      };
    } | null
  )?.response?.status;
}

async function groupOperations(
  ops: OutboxOp[]
): Promise<Map<string, OutboxOp[]>> {
  const groups = new Map<string, OutboxOp[]>();

  for (const op of ops) {
    const id = operationTaskId(op);
    const serverId = await resolveServerId(id);

    const key = serverId
      ? `server:${serverId}`
      : `local:${id}`;

    const group = groups.get(key) ?? [];

    group.push(op);
    groups.set(key, group);
  }

  return groups;
}

async function syncGroup(
  operations: OutboxOp[]
): Promise<void> {
  const createOp = operations.find(
    (op): op is Extract<OutboxOp, { op: "create" }> =>
      op.op === "create"
  );

  const hasDelete = operations.some(
    (op) => op.op === "delete"
  );

  const changes: TaskPatch = {};

  // Si quedó un create de un intento anterior,
  // también deben enviarse sus campos actuales.
  if (createOp) {
    Object.assign(changes, editableFields(createOp.data));
  }

  for (const op of operations) {
    if (op.op === "update") {
      Object.assign(changes, op.data);
    }
  }

  let serverId: string | undefined;

  for (const op of operations) {
    serverId = await resolveServerId(operationTaskId(op));

    if (serverId) break;
  }

  let createdNow = false;

  if (createOp && !serverId) {
    const response = await api.post<BulkSyncResponse>(
      "/tasks/bulksync",
      {
        tasks: [
          {
            ...changes,
            clienteId: createOp.clienteId,
          },
        ],
      }
    );

    const mapping = response.data.mapping?.find(
      (item) => item.clienteId === createOp.clienteId
    );

    if (!mapping || !isServerId(mapping.serverId)) {
      throw new Error(
        `El servidor no devolvió un ID válido para ${createOp.clienteId}`
      );
    }

    serverId = mapping.serverId;

    await promoteLocalToServer(
      createOp.clienteId,
      serverId
    );

    createdNow = true;
  }

  if (!serverId) {
    throw new Error(
      `No existe mapping ni creación pendiente para ${
        operationTaskId(operations[0])
      }`
    );
  }

  // También completar la promoción al recuperar
  // un create que ya tenía mapping.
  if (createOp && !createdNow) {
    await promoteLocalToServer(
      createOp.clienteId,
      serverId
    );
  }

  if (hasDelete) {
    // Incluso si se creó y eliminó offline, primero
    // resolvemos su ID para cubrir un POST anterior
    // cuya respuesta pudo haberse perdido.
    try {
      await api.delete(
        `/tasks/${encodeURIComponent(serverId)}`
      );
    } catch (error: unknown) {
      if (getHttpStatus(error) !== 404) {
        throw error;
      }
    }
  } else if (
    !createdNow &&
    Object.keys(changes).length > 0
  ) {
    await api.put(
      `/tasks/${encodeURIComponent(serverId)}`,
      changes
    );
  }

  // Solo confirma las revisiones enviadas.
  // Las revisiones nuevas permanecen en la outbox.
  await acknowledgeOperations(operations);
}

async function performSync(): Promise<void> {
  // Evita reintentar la misma revisión fallida
  // continuamente dentro de esta ejecución.
  const failed = new Set<string>();

  while (navigator.onLine) {
    const operations = await getOutbox();

    if (operations.length === 0) return;

    const groups = await groupOperations(operations);

    let processed = 0;

    for (const [key, group] of groups) {
      if (!navigator.onLine) return;

      const signature = JSON.stringify([
        key,
        group.map((op) => [op.id, op.rev]),
      ]);

      if (failed.has(signature)) continue;

      try {
        await syncGroup(group);
        processed++;
      } catch (error: unknown) {
        failed.add(signature);

        console.error(
          `Error sincronizando ${key}:`,
          error
        );
      }
    }

    // Si no hubo avances, dejar las operaciones
    // pendientes para el siguiente intento.
    if (processed === 0) return;

    // Volver a leer la outbox para procesar cambios
    // guardados mientras esperábamos al servidor.
  }
}

export function syncNow(): Promise<void> {
  requested = true;

  if (syncing) return syncing;

  syncing = (async () => {
    while (requested && navigator.onLine) {
      requested = false;
      await performSync();
    }
  })().finally(() => {
    syncing = null;
  });

  return syncing;
}

export function setupOnlineSync(): () => void {
  const handler = () => {
    void syncNow().catch((error: unknown) => {
      console.error(
        "Error general de sincronización:",
        error
      );
    });
  };

  window.addEventListener("online", handler);

  if (navigator.onLine) {
    handler();
  }

  return () => {
    window.removeEventListener("online", handler);
  };
}