---
title: 接口目录
hide:
  - navigation
  - toc
---

<section class="catalog-app" data-interface-catalog aria-label="eltdx 接口目录">
  <aside class="catalog-sidebar" aria-label="接口目录导航">
    <div class="catalog-sidebar-heading">
      <span class="catalog-sidebar-icon" aria-hidden="true">&lt;/&gt;</span>
      <strong>接口目录</strong>
    </div>
    <nav class="catalog-tree" data-interface-tree aria-label="接口层级"></nav>
    <nav class="catalog-reference-links" aria-label="参考文档">
      <span>参考文档</span>
      <a href="METHOD_REFERENCE/">调用方法</a>
      <a href="FIELD_REFERENCE/">字段手册</a>
      <a href="COMMANDS_7709/">命令映射</a>
    </nav>
  </aside>

  <div class="catalog-main">
    <header class="catalog-heading">
      <p class="catalog-kicker">接口目录</p>
      <h1 data-interface-heading>接口文档</h1>
      <p data-interface-lead>当前公开的二进制协议解析与上层调用能力。每项均链接到参数、返回字段和示例说明。</p>
    </header>

    <div class="interface-stats" data-interface-stats aria-label="接口层级统计"></div>

    <div class="interface-controls">
      <div class="interface-control-field interface-search-field">
        <label for="interface-search-input">搜索</label>
        <div class="interface-search">
          <input id="interface-search-input" type="search" data-interface-search autocomplete="off" placeholder="搜索接口、方法、命令号或 Entry">
        </div>
      </div>
      <label class="interface-control-field interface-scope-select">
        <span>目录</span>
        <select data-interface-scope-select aria-label="选择接口目录"></select>
      </label>
    </div>

    <p class="interface-result-meta"><strong data-interface-result-count aria-live="polite">0</strong><span> 项结果</span></p>

    <div class="interface-table" role="table" aria-label="eltdx 接口目录">
      <div class="interface-table-head" role="row">
        <span role="columnheader">接口 / 调用</span>
        <span role="columnheader">层级 / 目录</span>
        <span role="columnheader">协议 / 类型</span>
        <span role="columnheader">说明</span>
        <span role="columnheader">返回 / 文档</span>
      </div>
      <div class="interface-table-body" data-interface-rows role="rowgroup"></div>
      <p class="interface-empty" data-interface-empty hidden>没有匹配的接口。</p>
    </div>

    <noscript><p class="interface-empty">接口目录需要浏览器启用 JavaScript；其余文档仍可直接阅读。</p></noscript>

    <section class="catalog-scope" aria-labelledby="catalog-scope-title">
      <h2 id="catalog-scope-title">统计口径</h2>
      <p><code>二进制接口解析</code> 按 21 个已注册的 7709 命令组织；同一命令的编码、解码和对应方法只保留一份底层说明。</p>
      <p><code>上层接口</code> 收录 43 个面向调用者的能力：协议封装围绕协议处理分页、拆批和字段整理；Helpers 功能封装围绕实际场景组合接口并计算结果；MCP 工具负责向 Agent 暴露已有能力。</p>
      <p>目录数据随静态页面发布，打开后不会连接行情主站、F10 网关或任何 eltdx 后台服务。</p>
    </section>
  </div>
</section>
