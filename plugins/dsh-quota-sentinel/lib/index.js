/**
 * Quota Sentinel DSH Plugin
 * Multi-subscription quota guardian, automatic OmniRoute model router, and telemetry reporter.
 * Authored by Rachit Dev <rachit.dev@gmail.com>
 */

import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, appendFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

export const name = 'quota-sentinel-dsh-plugin';
export const inject = [];

const DEFAULT_BINARY = join(homedir(), '.dsh', 'bin', 'qs');
const DEFAULT_INTERVAL_MIN = 15;
const LOG_PATH = join(homedir(), '.dsh', 'logs', 'quota-sentinel.log');

function log(msg) {
  try {
    mkdirSync(join(homedir(), '.dsh', 'logs'), { recursive: true });
    appendFileSync(LOG_PATH, `[${new Date().toISOString()}] ${msg}\n`);
  } catch {}
}

export function runTick(binaryPath) {
  return new Promise((resolve) => {
    const bin = binaryPath || DEFAULT_BINARY;
    if (!existsSync(bin)) {
      log(`qs binary missing at ${bin}`);
      return resolve({ error: `binary missing at ${bin}` });
    }
    const proc = spawn(bin, ['tick', '--json'], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.stderr.on('data', (d) => { stderr += d; });
    proc.on('close', (code) => {
      log(`qs tick exit=${code} stdout=${stdout.trim()} stderr=${stderr.trim()}`);
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ code, stdout: stdout.trim(), stderr: stderr.trim() });
      }
    });
    proc.on('error', (err) => {
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
    // Initial tick 5s after boot
    const bootTimer = setTimeout(() => {
      runTick(binaryPath);
    }, 5000);
    bootTimer.unref?.();

    // Recurring 15-minute background tick inside DSH
    const intervalMs = intervalMin * 60 * 1000;
    const tickTimer = setInterval(() => {
      runTick(binaryPath);
    }, intervalMs);
    tickTimer.unref?.();

    // Daily 08:00 local report check (checked every 15 mins)
    let lastReportDate = '';
    const reportTimer = setInterval(() => {
      const now = new Date();
      const dateStr = now.toISOString().slice(0, 10);
      // In Asia/Kolkata (UTC+5:30) 08:00 local is 02:30 UTC
      // Or check local hours
      if (now.getHours() === 8 && lastReportDate !== dateStr) {
        lastReportDate = dateStr;
        log('firing daily 08:00 report via Resend...');
        runReport(binaryPath);
      }
    }, 60 * 1000);
    reportTimer.unref?.();

    ctx.on('dispose', () => {
      clearTimeout(bootTimer);
      clearInterval(tickTimer);
      clearInterval(reportTimer);
      log('quota-sentinel plugin disposed, timers cleared');
    });
  }

  // Register command on ctx.tools if available
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
