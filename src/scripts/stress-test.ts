/**
 * src/scripts/stress-test.ts
 * 
 * Enqueues a massive payload of tasks directly into the High-Performance Distributed Task Scheduler
 * and measures the raw throughput (Tasks Per Second) across the entire cluster.
 * 
 * Run with: npx ts-node src/scripts/stress-test.ts
 */

import "dotenv/config";
import { getDb, disconnectDb } from "../infra/db";
import { closeRedisClient } from "../infra/redis/redis-client";
import { createTask } from "../core/producer";

async function main() {
  const db = getDb();
  
  // Set how many tasks you want to blast into the system
  const TASK_COUNT = 5000;
  
  console.log(`\n============================================================`);
  console.log(`🚀 STARTING CLUSTER STRESS TEST`);
  console.log(`============================================================\n`);
  console.log(`Target: ${TASK_COUNT} Tasks`);
  
  const startTime = Date.now();

  // Enqueue in batches of 500 to avoid overwhelming the Node event loop on the client side
  for (let i = 0; i < TASK_COUNT; i += 500) {
    const promises = [];
    for (let j = 0; j < 500; j++) {
      if (i + j >= TASK_COUNT) break;
      
      promises.push(createTask({
        type: "email:send",
        payload: { stress: true, index: i + j, delayMs: 0 },
        priority: "HIGH"
      }));
    }
    await Promise.all(promises);
    process.stdout.write(`\r  Enqueued: ${Math.min(i + 500, TASK_COUNT)} / ${TASK_COUNT} tasks`);
  }

  const enqueueTime = Date.now() - startTime;
  console.log(`\n\n✅ Enqueueing Complete: Took ${enqueueTime}ms (${Math.round(TASK_COUNT / (enqueueTime / 1000))} tasks/sec enqueue rate)`);
  
  console.log(`\n⏳ Waiting for background workers to process all ${TASK_COUNT} tasks...`);
  
  // Count baseline completed tasks so we know how many we need to process
  const baselineCount = await db.task.count({ where: { status: "COMPLETED" } });
  const processStartTime = Date.now();

  while (true) {
    const currentCompleted = await db.task.count({ where: { status: "COMPLETED" } });
    const processed = currentCompleted - baselineCount;
    
    process.stdout.write(`\r  Processed: ${processed} / ${TASK_COUNT} tasks ...`);

    if (processed >= TASK_COUNT) {
      const processTime = Date.now() - processStartTime;
      const tps = Math.round(TASK_COUNT / (processTime / 1000));
      
      console.log(`\n\n🔥 STRESS TEST COMPLETE 🔥`);
      console.log(`============================================================`);
      console.log(`Total Processing Time : ${processTime}ms`);
      console.log(`Cluster Throughput    : ${tps} Tasks Per Second (TPS)`);
      console.log(`============================================================\n`);
      break;
    }
    
    // Poll every 250ms
    await new Promise(r => setTimeout(r, 250));
  }
}

main().finally(async () => {
  await disconnectDb();
  await closeRedisClient();
});
