---
title: 接口目录
hide:
  - toc
---

<header class="catalog-heading">
  <p class="catalog-kicker">纯静态文档 · 构建时快照</p>
  <h1>接口目录</h1>
  <p>当前公开的 7709 行情、7615 F10、Helper 与 MCP 调用入口。每项均链接到参数、返回字段和示例说明。</p>
</header>

<section class="interface-catalog" data-interface-catalog aria-label="eltdx 接口目录">
  <div class="interface-stats" data-interface-stats aria-label="接口能力统计"></div>

  <div class="interface-controls">
    <label class="interface-search">
      <span class="interface-visually-hidden">搜索接口</span>
      <input type="search" data-interface-search autocomplete="off" placeholder="搜索名称、方法、命令号或 Entry" aria-label="搜索接口">
    </label>
    <div class="interface-filters" data-interface-filters role="group" aria-label="按来源筛选"></div>
  </div>

  <p class="interface-result-meta"><strong data-interface-result-count aria-live="polite">0</strong><span> 项结果</span></p>

  <div class="interface-table" role="table" aria-label="eltdx 接口目录">
    <div class="interface-table-head" role="row">
      <span role="columnheader">接口 / 调用</span>
      <span role="columnheader">来源</span>
      <span role="columnheader">协议 / 类型</span>
      <span role="columnheader">说明</span>
      <span role="columnheader">返回 / 文档</span>
    </div>
    <div class="interface-table-body" data-interface-rows role="rowgroup"></div>
    <p class="interface-empty" data-interface-empty hidden>没有匹配的接口。</p>
  </div>

  <noscript><p class="interface-empty">接口目录需要浏览器启用 JavaScript；其余文档仍可直接阅读。</p></noscript>
</section>

<section class="catalog-scope" aria-labelledby="catalog-scope-title">
  <h2 id="catalog-scope-title">统计口径</h2>
  <p><code>7709</code> 按公开调用能力统计，当前底层注册 21 个二进制命令；ETF、指数与股票共用通用行情接口，不按证券类型重复计数。</p>
  <p>页面在构建时写入接口清单，打开后不会连接行情主站、F10 网关或任何 eltdx 后台服务。</p>
</section>
