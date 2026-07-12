(function () {
  "use strict";

  var root = document.querySelector("[data-interface-catalog]");
  if (!root) {
    return;
  }
  var catalog = window.ELTDX_CATALOG;
  if (!catalog || !Array.isArray(catalog.items)) {
    root.textContent = "接口目录数据加载失败。";
    return;
  }

  var items = catalog.items;
  var sourceLabels = {
    "7709": "7709",
    "F10": "7615 / F10",
    "Helper": "Helper",
    "MCP": "MCP"
  };
  var sourceOrder = ["7709", "F10", "Helper", "MCP"];
  var searchInput = root.querySelector("[data-interface-search]");
  var stats = root.querySelector("[data-interface-stats]");
  var filters = root.querySelector("[data-interface-filters]");
  var rows = root.querySelector("[data-interface-rows]");
  var resultCount = root.querySelector("[data-interface-result-count]");
  var empty = root.querySelector("[data-interface-empty]");
  var activeSource = "all";

  function createElement(tag, className, text) {
    var element = document.createElement(tag);
    if (className) {
      element.className = className;
    }
    if (text !== undefined) {
      element.textContent = text;
    }
    return element;
  }

  function countSource(source) {
    return items.filter(function (item) {
      return item.source === source;
    }).length;
  }

  function normalize(value) {
    return String(value || "")
      .normalize("NFKC")
      .toLocaleLowerCase()
      .replace(/\s+/g, " ")
      .trim();
  }

  function searchText(item) {
    return normalize([
      item.title,
      item.source,
      item.category,
      item.api,
      (item.aliases || []).join(" "),
      item.protocol,
      item.kind,
      item.summary,
      item.return_model
    ].join(" "));
  }

  function localDocUrl(item) {
    var relative = item.doc.replace(/\\/g, "/").replace(/\.md$/i, "/");
    return new URL(relative, window.location.href).href + (item.doc_anchor ? "#" + encodeURIComponent(item.doc_anchor) : "");
  }

  function renderStats() {
    sourceOrder.forEach(function (source) {
      var stat = createElement("div", "interface-stat");
      stat.appendChild(createElement("strong", "", String(countSource(source))));
      stat.appendChild(createElement("span", "", sourceLabels[source]));
      stats.appendChild(stat);
    });
  }

  function renderFilters() {
    [["all", "全部", items.length]].concat(sourceOrder.map(function (source) {
      return [source, sourceLabels[source], countSource(source)];
    })).forEach(function (definition) {
      var button = createElement("button", "interface-filter");
      button.type = "button";
      button.dataset.sourceFilter = definition[0];
      button.setAttribute("aria-pressed", String(definition[0] === activeSource));
      button.appendChild(createElement("span", "", definition[1]));
      button.appendChild(createElement("span", "interface-filter-count", String(definition[2])));
      button.addEventListener("click", function () {
        activeSource = definition[0];
        applyFilters();
      });
      filters.appendChild(button);
    });
  }

  function renderRows() {
    items.forEach(function (item) {
      var row = createElement("article", "interface-row");
      row.setAttribute("role", "row");
      row.dataset.interfaceItem = "";
      row.dataset.source = item.source;
      row.dataset.search = searchText(item);

      var name = createElement("div", "interface-cell interface-name");
      name.setAttribute("role", "cell");
      var titleLink = createElement("a", "", item.title);
      titleLink.href = localDocUrl(item);
      name.appendChild(titleLink);
      name.appendChild(createElement("code", "", item.api));

      var source = createElement("div", "interface-cell interface-source");
      source.setAttribute("role", "cell");
      var sourceTag = createElement("span", "interface-source-tag", sourceLabels[item.source]);
      sourceTag.dataset.source = item.source;
      source.appendChild(sourceTag);
      source.appendChild(createElement("small", "", item.category));

      var protocol = createElement("div", "interface-cell interface-protocol");
      protocol.setAttribute("role", "cell");
      protocol.appendChild(createElement("code", "", item.protocol));
      protocol.appendChild(createElement("small", "", item.kind));

      var description = createElement("div", "interface-cell interface-description", item.summary);
      description.setAttribute("role", "cell");

      var returned = createElement("div", "interface-cell interface-return");
      returned.setAttribute("role", "cell");
      returned.appendChild(createElement("code", "", item.return_model));
      var docLink = createElement("a", "", "文档");
      docLink.href = localDocUrl(item);
      docLink.setAttribute("aria-label", "查看 " + item.title + " 文档");
      returned.appendChild(docLink);

      row.appendChild(name);
      row.appendChild(source);
      row.appendChild(protocol);
      row.appendChild(description);
      row.appendChild(returned);
      rows.appendChild(row);
    });
  }

  function applyFilters() {
    var terms = normalize(searchInput.value).split(" ").filter(Boolean);
    var visible = 0;
    Array.prototype.forEach.call(rows.children, function (row) {
      var matchesSource = activeSource === "all" || row.dataset.source === activeSource;
      var matchesQuery = terms.every(function (term) {
        return row.dataset.search.indexOf(term) >= 0;
      });
      row.hidden = !(matchesSource && matchesQuery);
      if (!row.hidden) {
        visible += 1;
      }
    });
    Array.prototype.forEach.call(filters.children, function (button) {
      button.setAttribute("aria-pressed", String(button.dataset.sourceFilter === activeSource));
    });
    resultCount.textContent = String(visible);
    empty.hidden = visible !== 0;
  }

  renderStats();
  renderFilters();
  renderRows();
  searchInput.addEventListener("input", applyFilters);
  applyFilters();
})();
