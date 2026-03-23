/**
 * Copyright (c) Microsoft Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import crypto from 'crypto';
import fs from 'fs';
import net from 'net';
import path from 'path';

import * as playwright from '../../..';
import { registryDirectory } from '../../server/registry/index';
import { testDebug } from './log';
import { outputDir } from '../backend/context';
import { createExtensionBrowser } from './extensionContextFactory';
import { connectToBrowserAcrossVersions } from '../utils/connect';
import { serverRegistry } from '../../serverRegistry';
// eslint-disable-next-line no-restricted-imports
import { connectToBrowser } from '../../client/connect';

import type { FullConfig } from './config';
import type { ClientInfo } from '../utils/mcp/server';
// eslint-disable-next-line no-restricted-imports
import type { Playwright } from '../../client/playwright';
// eslint-disable-next-line no-restricted-imports
import type { Browser } from '../../client/browser';
import type { BrowserInfo } from '../../serverRegistry';

type ClientInfoEx = ClientInfo & {
  sessionName?: string;
  workspaceDir?: string;
};

type BrowserWithInfo = {
  browser: playwright.Browser,
  browserInfo: BrowserInfo
};

export async function createBrowser(config: FullConfig, clientInfo: ClientInfoEx): Promise<playwright.Browser> {
  const { browser } = await createBrowserWithInfo(config, clientInfo);
  return browser;
}

export async function createBrowserWithInfo(config: FullConfig, clientInfo: ClientInfoEx): Promise<BrowserWithInfo> {
  if (config.browser.remoteEndpoint)
    return await createRemoteBrowser(config);

  let browser: playwright.Browser;
  if (config.browser.cdpEndpoint)
    browser = await createCDPBrowser(config, clientInfo);
  else if (config.browser.isolated)
    browser = await createIsolatedBrowser(config, clientInfo);
  else if (config.extension)
    browser = await createExtensionBrowser(config, clientInfo);
  else
    browser = await createPersistentBrowser(config, clientInfo);

  return { browser, browserInfo: browserInfo(browser, config) };
}

export interface BrowserContextFactory {
  contexts(clientInfo: ClientInfo): Promise<playwright.BrowserContext[]>;
  createContext(clientInfo: ClientInfo): Promise<playwright.BrowserContext>;
}

// =====================================================================
// [新增功能]: CDP 拦截器配置函数
// 作用：通过 Chrome DevTools Protocol (CDP) 在底层接管网络请求。
// 实现了两个核心能力：
// 1. 代理服务器的自动静默鉴权（免弹窗输入账号密码）。
// 2. 根据 config.network.blockedOrigins 拦截并丢弃不合规的网络请求。
// =====================================================================
export async function setupCDPInterception(context: playwright.BrowserContext, config: FullConfig) {
  // 从配置中提取代理信息 (username, password) 和需要拦截的域名列表
  const proxyConfig = config.browser.launchOptions?.proxy as any;
  const blockedOrigins = config.network?.blockedOrigins || [];

  const attachInterceptor = async (page: playwright.Page) => {
    try {
      const cdpSession = await context.newCDPSession(page);
      
      // [新增]: 启用 CDP 的 Fetch 域
      // handleAuthRequests: 允许我们接管 401/407 的鉴权请求
      // patterns: 设置网络请求在发出的 'Request' 阶段就暂停，等待我们处理
      await cdpSession.send('Fetch.enable', { 
        handleAuthRequests: true,
        patterns: [{ requestStage: 'Request' }]
      });

      // [新增]: 监听需要鉴权的事件 (例如 HTTP 407 代理验证)
      cdpSession.on('Fetch.authRequired', async event => {
        const isProxy = event.authChallenge.source === 'Proxy';
        
        // 自动填入 proxyConfig 中解析出来的账号密码
        await cdpSession.send('Fetch.continueWithAuth', {
          requestId: event.requestId,
          authChallengeResponse: isProxy ? {
            response: 'ProvideCredentials',
            username: proxyConfig?.username || '',
            password: proxyConfig?.password || '',
          } : {
            response: 'Default' // 如果不是代理鉴权（如普通的网页 401），则走默认行为
          }
        }).catch(() => {});
      });

      // [新增]: 监听被暂停的普通网络请求
      cdpSession.on('Fetch.requestPaused', async event => {
        const requestUrl = event.request.url;

        // 检查当前请求的 URL 是否命中了我们配置的黑名单 (blockedOrigins)
        const shouldBlock = blockedOrigins.some((origin: string) => requestUrl.includes(origin));
        if (shouldBlock) {
          // 如果命中黑名单，直接在底层让这个请求失败，前端会看到 (Blocked by Client)
          await cdpSession.send('Fetch.failRequest', {
            requestId: event.requestId,
            errorReason: 'BlockedByClient'
          }).catch(() => {});
          return; // 终止函数，不再放行
        }

        // 如果一切正常，没有命中黑名单，则放行该请求让浏览器继续加载
        await cdpSession.send('Fetch.continueRequest', {
          requestId: event.requestId,
        }).catch(() => {});
      });

    } catch {
      // Ignore session creation failures on detached pages
    }
  };

  // 为当前已有页面绑定 CDP 拦截器
  context.on('page', attachInterceptor);
  // 为未来可能打开的所有新页面绑定 CDP 拦截器
  await Promise.all(context.pages().map(attachInterceptor));
}

function browserInfo(browser: playwright.Browser, config: FullConfig): BrowserInfo {
  return {
    // eslint-disable-next-line no-restricted-syntax
    guid: (browser as any)._guid,
    browserName: config.browser.browserName,
    launchOptions: config.browser.launchOptions,
    userDataDir: config.browser.userDataDir
  };
}

async function createIsolatedBrowser(config: FullConfig, clientInfo: ClientInfoEx): Promise<playwright.Browser> {
  testDebug('create browser (isolated)');
  await injectCdpPort(config.browser);
  const browserType = playwright[config.browser.browserName];
  const tracesDir = await computeTracesDir(config, clientInfo);
  const browser = await browserType.launch({
    tracesDir,
    ...config.browser.launchOptions,
    handleSIGINT: false,
    handleSIGTERM: false,
  }).catch(error => {
    if (error.message.includes('Executable doesn\'t exist'))
      throwBrowserIsNotInstalledError(config);
    throw error;
  });
  await startServer(browser, clientInfo);
  return browser;
}

async function createCDPBrowser(config: FullConfig, clientInfo: ClientInfoEx): Promise<playwright.Browser> {
  testDebug('create browser (cdp)');
  const browser = await playwright.chromium.connectOverCDP(config.browser.cdpEndpoint!, {
    headers: config.browser.cdpHeaders,
    timeout: config.browser.cdpTimeout
  });
  await startServer(browser, clientInfo);
  return browser;
}

async function createRemoteBrowser(config: FullConfig): Promise<BrowserWithInfo> {
  testDebug('create browser (remote)');
  const descriptor = await serverRegistry.find(config.browser.remoteEndpoint!);
  if (descriptor) {
    const browser = await connectToBrowserAcrossVersions(descriptor);
    return {
      browser,
      browserInfo: {
        guid: descriptor.browser.guid,
        browserName: descriptor.browser.browserName,
        launchOptions: descriptor.browser.launchOptions,
        userDataDir: descriptor.browser.userDataDir
      }
    };
  }

  const endpoint = config.browser.remoteEndpoint!;
  const playwrightObject = playwright as Playwright;
  // Use connectToBrowser instead of playwright[browserName].connect because we don't have browserName.
  const browser = await connectToBrowser(playwrightObject, { endpoint });
  browser._connectToBrowserType(playwrightObject[browser._browserName], {}, undefined);
  return { browser, browserInfo: browserInfo(browser, config) };
}

async function createPersistentBrowser(config: FullConfig, clientInfo: ClientInfoEx): Promise<playwright.Browser> {
  testDebug('create browser (persistent)');
  await injectCdpPort(config.browser);
  const userDataDir = config.browser.userDataDir ?? await createUserDataDir(config, clientInfo);
  const tracesDir = await computeTracesDir(config, clientInfo);

  if (await isProfileLocked5Times(userDataDir))
    throw new Error(`Browser is already in use for ${userDataDir}, use --isolated to run multiple instances of the same browser`);

  const browserType = playwright[config.browser.browserName];
  const launchOptions: playwright.LaunchOptions & playwright.BrowserContextOptions = {
    tracesDir,
    ...config.browser.launchOptions,
    ...config.browser.contextOptions,
    handleSIGINT: false,
    handleSIGTERM: false,
    ignoreDefaultArgs: [
      '--disable-extensions',
    ],
  };
  try {
    const browserContext = await browserType.launchPersistentContext(userDataDir, launchOptions);
    
    // =====================================================================
    // [新增]: 挂载 CDP 拦截器
    // 在浏览器上下文 (BrowserContext) 创建成功后，立刻调用 setupCDPInterception，
    // 把代理配置和黑名单配置注入进去，确保在加载任何网页前就生效。
    // =====================================================================
    await setupCDPInterception(browserContext, config);

    const browser = browserContext.browser()!;
    await startServer(browser, clientInfo);
    return browser;
  } catch (error: any) {
    if (error.message.includes('Executable doesn\'t exist'))
      throwBrowserIsNotInstalledError(config);
    if (error.message.includes('cannot open shared object file: No such file or directory')) {
      const browserName = launchOptions.channel ?? config.browser.browserName;
      throw new Error(`Missing system dependencies required to run browser ${browserName}. Install them with: sudo npx playwright install-deps ${browserName}`);
    }
    if (error.message.includes('ProcessSingleton') || error.message.includes('exitCode=21'))
      throw new Error(`Browser is already in use for ${userDataDir}, use --isolated to run multiple instances of the same browser`);
    throw error;
  }
}

async function createUserDataDir(config: FullConfig, clientInfo: ClientInfo) {
  const dir = process.env.PWMCP_PROFILES_DIR_FOR_TEST ?? registryDirectory;
  const browserToken = config.browser.launchOptions?.channel ?? config.browser?.browserName;
  // Hesitant putting hundreds of files into the user's workspace, so using it for hashing instead.
  const rootPathToken = createHash(clientInfo.cwd);
  const result = path.join(dir, `mcp-${browserToken}-${rootPathToken}`);
  await fs.promises.mkdir(result, { recursive: true });
  return result;
}

async function injectCdpPort(browserConfig: FullConfig['browser']) {
  if (browserConfig.browserName === 'chromium')
    // eslint-disable-next-line no-restricted-syntax
    (browserConfig.launchOptions as any).cdpPort = await findFreePort();
}

async function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
    server.on('error', reject);
  });
}

function createHash(data: string): string {
  return crypto.createHash('sha256').update(data).digest('hex').slice(0, 7);
}

async function computeTracesDir(config: FullConfig, clientInfo: ClientInfo): Promise<string | undefined> {
  return path.resolve(outputDir({ config, cwd: clientInfo.cwd }), 'traces');
}

async function isProfileLocked5Times(userDataDir: string): Promise<boolean> {
  for (let i = 0; i < 5; i++) {
    if (!isProfileLocked(userDataDir))
      return false;
    await new Promise(f => setTimeout(f, 1000));
  }
  return true;
}

export function isProfileLocked(userDataDir: string): boolean {
  const lockFile = process.platform === 'win32' ? 'lockfile' : 'SingletonLock';
  const lockPath = path.join(userDataDir, lockFile);

  if (process.platform === 'win32') {
    try {
      const fd = fs.openSync(lockPath, 'r+');
      fs.closeSync(fd);
      return false;
    } catch (e: any) {
      return e.code !== 'ENOENT';
    }
  }

  try {
    const target = fs.readlinkSync(lockPath);
    const pid = parseInt(target.split('-').pop() || '', 10);
    if (isNaN(pid))
      return false;
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function throwBrowserIsNotInstalledError(config: FullConfig): never {
  const channel = config.browser.launchOptions?.channel ?? config.browser.browserName;
  if (config.skillMode)
    throw new Error(`Browser "${channel}" is not installed. Run \`playwright-cli install-browser ${channel}\` to install`);
  else
    throw new Error(`Browser "${channel}" is not installed. Run \`npx @playwright/mcp install-browser ${channel}\` to install`);
}

async function startServer(browser: playwright.Browser, clientInfo: ClientInfoEx) {
  if (clientInfo.sessionName)
    await (browser as Browser)._register(clientInfo.sessionName, { workspaceDir: clientInfo.workspaceDir });
}