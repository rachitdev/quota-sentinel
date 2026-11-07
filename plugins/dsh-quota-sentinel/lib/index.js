/**
 * Quota Sentinel DSH Plugin
 * Multi-subscription quota guardian, automatic OmniRoute model router, and telemetry reporter.
 * Authored by Rachit Dev <rachit.dev@gmail.com>
 */

import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

export const name = 'quota-sentinel-dsh-plugin';
export const inject = [];

const DEFAULT_BINARY = join(homedir(), '.dsh', 'bin', 'qs');
const DEFAULT_INTERVAL_MIN = 15;
const STATE_DIR = join(homedir(), '.dsh', 'state', 'quota-sentinel');
const LOG_PATH = join(homedir(), '.dsh', 'logs', 'quota-sentinel.log');
const REPORT_STAMP_PATH = join(STATE_DIR, 'last_report_date.txt');

function log(msg) {
  try {
    mkdirSync(join(homedir(), '.dsh', 'logs'), { recursive: true });
    appendFileSync(LOG_PATH, `[${new Date().toISOString()}] ${msg}\n`);
  } catch {}
}

function getLocalDateString(timeZone = 'Asia/Kolkata') {
  try {
    return new Intl.DateTimeFormat('en-CA', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  } catch {
    return new Date().toISOString().slice(0, 10);
  }
}

function getLocalHour(timeZone = 'Asia/Kolkata') {
  try {
    const parts = new Intl.DateTimeFormat('en-US', { timeZone, hour: 'numeric', hour12: false }).formatToParts(new Date());
    return parseInt(parts.find((p) => p.type === 'hour')?.value ?? '0', 10);
  } catch {
    return new Date().getHours();
  }
}

let activeChild = null;

export function runTick(binaryPath) {
  return new Promise((resolve) => {
    const bin = binaryPath || DEFAULT_BINARY;
    if (!existsSync(bin)) {
      log(`qs binary missing at ${bin}`);
      return resolve({ error: `binary missing at ${bin}` });
    }
    const proc = spawn(bin, ['tick', '--json'], { stdio: ['ignore', 'pipe', 'pipe'] });
    activeChild = proc;
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.stderr.on('data', (d) => { stderr += d; });
    proc.on('close', (code) => {
      if (activeChild === proc) activeChild = null;
      log(`qs tick exit=${code} stdout=${stdout.trim()} stderr=${stderr.trim()}`);
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ code, stdout: stdout.trim(), stderr: stderr.trim() });
      }
    });
    proc.on('error', (err) => {
      if (activeChild === proc) activeChild = null;
      log(`qs tick spawn error: ${err.message}`);
      resolve({ error: err.message });
    });
  });
}

export function runReport(binaryPath) {
  return new Promise((resolve) => {
    const bin = binaryPath || DEFAULT_BINARY;
    if (!existsSync(bin)) {
      log(`qs binary missing at ${bin}`);
      return resolve({ error: `binary missing at ${bin}` });
    }
    const proc = spawn(bin, ['report'], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.stderr.on('data', (d) => { stderr += d; });
    proc.on('close', (code) => {
      log(`qs report exit=${code} stdout=${stdout.trim()} stderr=${stderr.trim()}`);
      resolve({ code, stdout: stdout.trim(), stderr: stderr.trim() });
    });
    proc.on('error', (err) => {
      log(`qs report spawn error: ${err.message}`);
      resolve({ error: err.message });
    });
  });
}

export function apply(ctx, config = {}) {
  const binaryPath = config.binaryPath || DEFAULT_BINARY;
  const intervalMin = config.tickIntervalMinutes || DEFAULT_INTERVAL_MIN;
  const autoTick = config.autoTick !== false;

  log(`quota-sentinel plugin applied (binary=${binaryPath}, autoTick=${autoTick}, interval=${intervalMin}m)`);

  ctx.quotaSentinel = {
    runTick: () => runTick(binaryPath),
    runReport: () => runReport(binaryPath),
    getBinaryPath: () => binaryPath,
  };

  if (autoTick) {
    let lastTickTime = 0;

    // Boot-up initial tick after 5 seconds to let OmniRoute settle
    const bootTimer = setTimeout(() => {
      lastTickTime = Date.now();
      runTick(binaryPath);
    }, 5000);
    bootTimer.unref?.();

    // Sleep/wake drift resilient watchdog: checks every 60s
    // If >= intervalMin has elapsed since last tick, trigger tick immediately
    const checkTimer = setInterval(() => {
      const now = Date.now();
      const intervalMs = intervalMin * 60 * 1000;
      if (now - lastTickTime >= intervalMs) {
        lastTickTime = now;
        runTick(binaryPath);
      }

      // Timezone-explicit daily 08:00 catch-up report
      const localHour = getLocalHour('Asia/Kolkata');
      const todayStr = getLocalDateString('Asia/Kolkata');
      if (localHour >= 8) {
        let lastReport = '';
        try {
          if (existsSync(REPORT_STAMP_PATH)) {
            lastReport = readFileSync(REPORT_STAMP_PATH, 'utf8').trim();
          }
        } catch {}

        if (lastReport !== todayStr) {
          try {
            mkdirSync(STATE_DIR, { recursive: true });
            writeFileSync(REPORT_STAMP_PATH, todayStr, 'utf8');
          } catch {}
          log(`triggering daily report for ${todayStr} (localHour=${localHour})...`);
          runReport(binaryPath);
        }
      }
    }, 60 * 1000);
    checkTimer.unref?.();

    ctx.effect(() => {
      return () => {
        clearTimeout(bootTimer);
        clearInterval(checkTimer);
        if (activeChild) {
          try { activeChild.kill('SIGTERM'); } catch {}
          activeChild = null;
        }
        log('quota-sentinel plugin disposed, timers cleared and active child terminated');
      };
    });
  }

  // Register native tool on ctx.tools if available
  const tools = ctx.get('tools');
  if (tools && typeof tools.register === 'function') {
    ctx.effect(() => tools.register({
      name: 'quota_sentinel_tick',
      description: 'Trigger an immediate Quota Sentinel quota evaluation tick in OmniRoute.',
      parameters: { type: 'object', properties: {} },
      execute: async () => {
        const res = await runTick(binaryPath);
        return [{ type: 'text', text: JSON.stringify(res, null, 2) }];
      },
    }));
  }
}
