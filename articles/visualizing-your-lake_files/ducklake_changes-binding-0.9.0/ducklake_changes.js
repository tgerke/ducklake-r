/* Renderer for view_table_changes(). The payload is computed in R (see
   build_changes_payload() in R/view_table_changes.R): update pairs, changed
   cells, counts, and schema events all arrive ready to draw. Data frames
   arrive column-major (x.items.kind[i], x.items.rowid[i]); items.old and
   items.new are 1-based positions in the x.data columns. Every string goes
   into the page through textContent, never as HTML. */
(function () {
  "use strict";

  var KINDS = ["insert", "update", "delete"];
  var KIND_LABEL = { insert: "inserted", update: "updated", "delete": "deleted" };
  var STATUS_KIND = {
    added: "insert", created: "insert", dropped: "delete",
    renamed: "update", type_changed: "update", altered: "update"
  };
  var EVENT_LABEL = {
    added: "added", created: "created", dropped: "dropped",
    renamed: "renamed", type_changed: "type changed", altered: "altered"
  };
  var ARROW = " → ";
  var DOT = " · ";
  var PAGE_SIZE = 200;
  var CLIP = 120;

  HTMLWidgets.widget({
    name: "ducklake_changes",
    type: "output",
    factory: function (el) {
      return {
        renderValue: function (x) {
          var state = createState(x);
          el.classList.add("ducklake-changes");
          buildShell(el, state);
          render(state);
        },
        resize: function () {
          // The layout is flex and fills the element; nothing to recompute
        }
      };
    }
  });

  /* ---- generic helpers -------------------------------------------------- */

  function h(tag, attrs, children) {
    var node = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (key) {
        var value = attrs[key];
        if (value === null || value === undefined || value === false) return;
        if (key === "className") node.className = value;
        else if (key === "onclick") node.addEventListener("click", value);
        else if (key === "oninput") node.addEventListener("input", value);
        else node.setAttribute(key, value);
      });
    }
    append(node, children);
    return node;
  }

  function append(node, children) {
    if (children === null || children === undefined) return;
    if (!Array.isArray(children)) children = [children];
    children.forEach(function (child) {
      if (child === null || child === undefined || child === false) return;
      if (typeof child === "string" || typeof child === "number") {
        node.appendChild(document.createTextNode(String(child)));
      } else {
        node.appendChild(child);
      }
    });
  }

  function isNull(value) {
    return value === null || value === undefined;
  }

  function nrow(df) {
    var keys = Object.keys(df || {});
    return keys.length ? df[keys[0]].length : 0;
  }

  function range(n) {
    var out = [];
    for (var i = 0; i < n; i++) out.push(i);
    return out;
  }

  function fmtInt(n) {
    return Number(n).toLocaleString("en-US");
  }

  function plural(n, word) {
    return fmtInt(n) + " " + word + (n === 1 ? "" : "s");
  }

  function clip(s) {
    return s.length > CLIP ? s.slice(0, CLIP - 1) + "…" : s;
  }

  function naText(value) {
    return isNull(value) ? "NA" : String(value);
  }

  /* ---- state and index -------------------------------------------------- */

  function createState(x) {
    var index = buildIndex(x);
    return {
      x: x,
      index: index,
      view: defaultView(x, index),
      side: "new",
      changedOnly: true,
      schemaAll: false,
      filter: "",
      page: 0,
      dom: {}
    };
  }

  function buildIndex(x) {
    var items = x.items;
    var n = nrow(items);
    var byKind = { insert: [], update: [], "delete": [] };
    var partial = [];
    var bySnapshot = {};
    var i, j;
    for (i = 0; i < n; i++) {
      byKind[items.kind[i]].push(i);
      if (items.partial[i]) partial.push(i);
      var s = String(items.snapshot_id[i]);
      if (!bySnapshot[s]) bySnapshot[s] = [];
      bySnapshot[s].push(i);
    }
    var changedByItem = [];
    for (i = 0; i < n; i++) changedByItem.push({});
    var cellsByColumn = {};
    var cells = x.cells;
    for (j = 0; j < nrow(cells); j++) {
      var item = cells.item[j] - 1;
      var col = cells.column[j];
      changedByItem[item][col] = true;
      if (!cellsByColumn[col]) cellsByColumn[col] = [];
      cellsByColumn[col].push(j);
    }
    var dataCols = x.columns.name || [];
    var typeByCol = {};
    var columnsChanged = [];
    for (i = 0; i < dataCols.length; i++) {
      typeByCol[dataCols[i]] = x.columns.type[i];
      if (x.columns.changed_cells[i] > 0) {
        columnsChanged.push({ name: dataCols[i], count: x.columns.changed_cells[i] });
      }
    }
    columnsChanged.sort(function (a, b) { return b.count - a.count; });
    var schemaChanges = [];
    var events = x.schema.events;
    for (j = 0; j < nrow(events); j++) {
      if (events.kind[j] !== "created") schemaChanges.push(j);
    }
    var snapshotById = {};
    for (i = 0; i < nrow(x.snapshots); i++) {
      snapshotById[String(x.snapshots.snapshot_id[i])] = i;
    }
    return {
      byKind: byKind,
      partial: partial,
      bySnapshot: bySnapshot,
      changedByItem: changedByItem,
      cellsByColumn: cellsByColumn,
      dataCols: dataCols,
      typeByCol: typeByCol,
      columnsChanged: columnsChanged,
      schemaChanges: schemaChanges,
      snapshotById: snapshotById
    };
  }

  function defaultView(x, index) {
    if (x.schema.available && index.schemaChanges.length) return { kind: "schema", key: null };
    if (index.byKind.update.length) return { kind: "rows", key: "update" };
    if (index.byKind.insert.length) return { kind: "rows", key: "insert" };
    if (index.byKind["delete"].length) return { kind: "rows", key: "delete" };
    return { kind: "cells", key: null };
  }

  function sameView(a, b) {
    return a.kind === b.kind && String(a.key) === String(b.key);
  }

  function setView(state, view) {
    state.view = view;
    state.page = 0;
    render(state);
  }

  /* ---- cell access -------------------------------------------------------- */

  function cellValue(x, col, idx) {
    if (isNull(idx)) return null;
    var column = x.data[col];
    if (!column) return null;
    var value = column[idx - 1];
    return isNull(value) ? null : String(value);
  }

  function cellText(x, col, idx) {
    return naText(cellValue(x, col, idx));
  }

  function fillCell(td, value) {
    if (isNull(value)) {
      td.textContent = "NA";
      td.classList.add("dlc-na");
    } else {
      td.textContent = clip(value);
      if (value.length > CLIP) td.title = value;
    }
  }

  function colClass(index, col) {
    var type = index.typeByCol[col];
    return type === "integer" || type === "number" ? "dlc-num" : null;
  }

  function swatch(kind, partial) {
    var cls = "dlc-swatch dlc-swatch-" + (partial ? "partial" : kind);
    return h("span", { className: cls });
  }

  /* ---- shell -------------------------------------------------------------- */

  function buildShell(el, state) {
    var x = state.x;
    el.textContent = "";
    var subtitle = [];
    if (!isNull(x.ducklake_name)) subtitle.push("lake " + x.ducklake_name);
    var rangeLabel = rangeText(x.range);
    if (rangeLabel) subtitle.push(rangeLabel);

    var legend = h("div", { className: "dlc-legend" }, KINDS.map(function (kind) {
      return h("span", { className: "dlc-legend-item" }, [swatch(kind), KIND_LABEL[kind]]);
    }));
    var filter = h("input", {
      className: "dlc-filter",
      type: "search",
      placeholder: "Filter rows",
      "aria-label": "Filter rows",
      oninput: function (event) {
        state.filter = event.target.value;
        state.page = 0;
        renderMain(state);
      }
    });
    var header = h("div", { className: "dlc-header" }, [
      h("div", null, [
        h("div", { className: "dlc-title" }, x.title),
        subtitle.length ? h("div", { className: "dlc-subtitle" }, subtitle.join(DOT)) : null
      ]),
      legend,
      filter
    ]);
    var banner = null;
    if (x.truncated) {
      banner = h("div", { className: "dlc-banner" },
        "Showing the first " + fmtInt(x.counts.rows_shown) + " of " +
        fmtInt(x.counts.rows_total) + " feed rows. Filter the feed with dplyr to narrow it.");
    }
    state.dom.sidebar = h("nav", { className: "dlc-sidebar" });
    state.dom.toolbar = h("div", { className: "dlc-toolbar" });
    state.dom.main = h("div", { className: "dlc-main" });
    var body = h("div", { className: "dlc-body" }, [
      state.dom.sidebar,
      h("div", { className: "dlc-panel" }, [state.dom.toolbar, state.dom.main])
    ]);
    el.appendChild(h("div", { className: "dlc-app" }, [header, banner, body]));
  }

  function rangeText(r) {
    if (!r || isNull(r.start_id)) return "";
    if (r.bound_type === "timestamp") return r.start + " to " + r.end + " (UTC)";
    if (r.start_id === r.end_id) return "snapshot " + r.start_id;
    return "snapshots " + r.start_id + " to " + r.end_id;
  }

  function render(state) {
    renderSidebar(state);
    renderToolbar(state);
    renderMain(state);
  }

  /* ---- sidebar ------------------------------------------------------------ */

  function entry(state, label, count, view, opts) {
    opts = opts || {};
    var cls = "dlc-entry" + (opts.sub ? " sub" : "") +
      (sameView(state.view, view) ? " active" : "");
    return h("div", {
      className: cls,
      title: opts.title || null,
      onclick: function () { setView(state, view); }
    }, [
      h("span", { className: "dlc-label" + (opts.mono ? " dlc-mono" : "") }, label),
      h("span", { className: "dlc-count" }, fmtInt(count))
    ]);
  }

  function group(label, count) {
    return h("div", { className: "dlc-group" }, [
      label,
      isNull(count) ? null : h("span", { className: "dlc-count" }, fmtInt(count))
    ]);
  }

  function snapshotTitle(x, i) {
    var s = x.snapshots;
    var author = isNull(s.author[i]) ? "" : s.author[i];
    var message = isNull(s.commit_message[i]) ? "" : s.commit_message[i];
    if (author && message) return author + ": " + message;
    return author || message || "";
  }

  function renderSidebar(state) {
    var x = state.x;
    var index = state.index;
    var nav = state.dom.sidebar;
    nav.textContent = "";

    if (x.schema.available && index.schemaChanges.length) {
      nav.appendChild(entry(state, "Schema", index.schemaChanges.length, { kind: "schema", key: null }));
    }
    if (index.columnsChanged.length) {
      nav.appendChild(group("Columns changed", index.columnsChanged.length));
      index.columnsChanged.forEach(function (c) {
        nav.appendChild(entry(state, c.name, c.count, { kind: "cells", key: c.name },
          { sub: true, mono: true, title: plural(c.count, "changed cell") + " in " + c.name }));
      });
    }
    var anyRows = KINDS.some(function (kind) { return index.byKind[kind].length > 0; });
    if (anyRows) {
      nav.appendChild(group("Rows", null));
      KINDS.forEach(function (kind) {
        if (!index.byKind[kind].length) return;
        nav.appendChild(entry(state, "Rows " + KIND_LABEL[kind], index.byKind[kind].length,
          { kind: "rows", key: kind }));
        if (kind === "update" && index.partial.length) {
          nav.appendChild(entry(state, "one side only", index.partial.length,
            { kind: "rows", key: "partial" },
            { sub: true, title: "Updates with only one image in the feed" }));
        }
      });
    }
    if (nrow(x.cells)) {
      nav.appendChild(entry(state, "Cells changed", nrow(x.cells), { kind: "cells", key: null }));
    }
    if (nrow(x.snapshots)) {
      nav.appendChild(group("Snapshots", nrow(x.snapshots)));
      for (var i = 0; i < nrow(x.snapshots); i++) {
        var id = x.snapshots.snapshot_id[i];
        var count = x.snapshots.inserted[i] + x.snapshots.updated[i] + x.snapshots.deleted[i];
        var label = "#" + id + (isNull(x.snapshots.time[i]) ? "" : "  " + x.snapshots.time[i]);
        nav.appendChild(entry(state, label, count, { kind: "snapshot", key: String(id) },
          { sub: true, mono: true, title: snapshotTitle(x, i) || null }));
      }
    }
    if (!nav.childNodes.length) {
      nav.appendChild(h("div", { className: "dlc-empty" }, "The feed is empty."));
    }
  }

  /* ---- toolbar ------------------------------------------------------------ */

  function segmented(options, current, onChange) {
    var box = h("div", { className: "dlc-segmented" });
    options.forEach(function (option) {
      var button = h("button", {
        type: "button",
        className: option.value === current ? "active" : null
      }, option.label);
      button.addEventListener("click", function () {
        Array.prototype.forEach.call(box.children, function (b) { b.classList.remove("active"); });
        button.classList.add("active");
        onChange(option.value);
      });
      box.appendChild(button);
    });
    return box;
  }

  function viewItems(state) {
    var v = state.view;
    var index = state.index;
    if (v.kind === "rows") return v.key === "partial" ? index.partial : index.byKind[v.key];
    if (v.kind === "snapshot") return index.bySnapshot[v.key] || [];
    return [];
  }

  function viewTitle(state) {
    var v = state.view;
    if (v.kind === "schema") return "Schema";
    if (v.kind === "rows") {
      return v.key === "partial" ? "Updates with one side only" : "Rows " + KIND_LABEL[v.key];
    }
    if (v.kind === "snapshot") return "Snapshot #" + v.key;
    return v.key ? "Cells changed in " + v.key : "Cells changed";
  }

  function renderToolbar(state) {
    var toolbar = state.dom.toolbar;
    toolbar.textContent = "";
    toolbar.appendChild(h("div", { className: "dlc-view-title" }, viewTitle(state)));
    var controls = h("div", { className: "dlc-controls" });
    var v = state.view;
    var hasUpdates = (v.kind === "rows" || v.kind === "snapshot") &&
      viewItems(state).some(function (i) {
        return state.x.items.kind[i] === "update" && !state.x.items.partial[i];
      });
    if (hasUpdates) {
      controls.appendChild(segmented(
        [{ value: "old", label: "old" }, { value: "new", label: "new" }],
        state.side,
        function (value) { state.side = value; renderMain(state); }
      ));
      controls.appendChild(segmented(
        [{ value: true, label: "changed columns" }, { value: false, label: "all columns" }],
        state.changedOnly,
        function (value) { state.changedOnly = value; renderMain(state); }
      ));
    }
    if (v.kind === "schema") {
      controls.appendChild(segmented(
        [{ value: false, label: "changes" }, { value: true, label: "all columns" }],
        state.schemaAll,
        function (value) { state.schemaAll = value; renderMain(state); }
      ));
    }
    toolbar.appendChild(controls);
  }

  /* ---- main panel --------------------------------------------------------- */

  function renderMain(state) {
    var main = state.dom.main;
    main.textContent = "";
    var v = state.view;
    if (v.kind === "schema") {
      renderSchemaView(state);
    } else if (v.kind === "snapshot") {
      renderSnapshotCard(state);
      renderRowsView(state, viewItems(state));
    } else if (v.kind === "rows") {
      renderRowsView(state, viewItems(state));
    } else {
      renderCellsView(state);
    }
  }

  function applyFilter(state, rows) {
    var q = state.filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(function (row) {
      return row.strings.join("\n").toLowerCase().indexOf(q) >= 0;
    });
  }

  function paginate(state, total) {
    var pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    if (state.page >= pages) state.page = pages - 1;
    if (state.page < 0) state.page = 0;
    var start = state.page * PAGE_SIZE;
    return { start: start, end: Math.min(total, start + PAGE_SIZE), pages: pages, total: total };
  }

  function renderPager(state, page) {
    var text = page.total === 0
      ? "No rows"
      : "rows " + fmtInt(page.start + 1) + "–" + fmtInt(page.end) + " of " + fmtInt(page.total);
    var prev = h("button", { type: "button", disabled: state.page === 0 ? "disabled" : null }, "Previous");
    var next = h("button", { type: "button", disabled: state.page >= page.pages - 1 ? "disabled" : null }, "Next");
    prev.addEventListener("click", function () { state.page -= 1; renderMain(state); });
    next.addEventListener("click", function () { state.page += 1; renderMain(state); });
    return h("div", { className: "dlc-pager" }, [
      h("span", null, text),
      page.pages > 1 ? prev : null,
      page.pages > 1 ? next : null
    ]);
  }

  function table(head, body) {
    return h("div", { className: "dlc-table-wrap" },
      h("table", { className: "dlc-table" }, [
        h("thead", null, h("tr", null, head)),
        h("tbody", null, body)
      ]));
  }

  function empty(state, message) {
    state.dom.main.appendChild(h("div", { className: "dlc-empty" }, message));
  }

  /* Rows view: one line per item, from the side the toggle selects */

  function markerTitle(x, row) {
    var text = KIND_LABEL[row.kind];
    if (row.partial) text += " (one side only)";
    else if (row.kind === "update" && x.items.n_changed[row.item] === 0) text += " (no cells changed)";
    return text;
  }

  function renderRowsView(state, itemIdxs) {
    var x = state.x;
    var index = state.index;
    if (!itemIdxs.length) {
      empty(state, "No rows in this view.");
      return;
    }
    var hasPairs = itemIdxs.some(function (i) {
      return x.items.kind[i] === "update" && !x.items.partial[i];
    });
    var cols = index.dataCols;
    if (hasPairs && state.changedOnly) {
      var changed = {};
      itemIdxs.forEach(function (i) {
        Object.keys(index.changedByItem[i]).forEach(function (c) { changed[c] = true; });
      });
      var narrowed = cols.filter(function (c) { return changed[c]; });
      if (narrowed.length) cols = narrowed;
    }
    var rows = itemIdxs.map(function (i) {
      var kind = x.items.kind[i];
      var oldI = x.items.old[i];
      var newI = x.items.new[i];
      var side = kind === "insert" ? "new"
        : kind === "delete" ? "old"
        : isNull(oldI) ? "new"
        : isNull(newI) ? "old"
        : state.side;
      var src = side === "new" ? newI : oldI;
      var strings = [String(x.items.snapshot_id[i]), String(x.items.rowid[i])];
      cols.forEach(function (c) { strings.push(cellText(x, c, src)); });
      return {
        item: i, kind: kind, oldI: oldI, newI: newI, src: src,
        partial: !!x.items.partial[i], strings: strings
      };
    });
    rows = applyFilter(state, rows);
    var page = paginate(state, rows.length);

    var head = [
      h("th", { className: "dlc-marker-col" }, ""),
      h("th", { className: "dlc-num" }, "snapshot"),
      h("th", { className: "dlc-num" }, "rowid")
    ];
    cols.forEach(function (c) { head.push(h("th", { className: colClass(index, c) }, c)); });

    var body = [];
    for (var r = page.start; r < page.end; r++) {
      var row = rows[r];
      var tr = h("tr", { className: "dlc-row-" + row.kind });
      tr.appendChild(h("td", { className: "dlc-marker-col", title: markerTitle(x, row) },
        swatch(row.kind, row.partial)));
      tr.appendChild(h("td", { className: "dlc-num" }, row.strings[0]));
      tr.appendChild(h("td", { className: "dlc-num" }, row.strings[1]));
      cols.forEach(function (c) {
        var td = h("td", { className: colClass(index, c) });
        fillCell(td, cellValue(x, c, row.src));
        if (row.kind === "update" && index.changedByItem[row.item][c]) {
          td.classList.add("dlc-changed");
          td.title = cellText(x, c, row.oldI) + ARROW + cellText(x, c, row.newI);
        }
        tr.appendChild(td);
      });
      body.push(tr);
    }
    state.dom.main.appendChild(table(head, body));
    state.dom.main.appendChild(renderPager(state, page));
  }

  /* Cells view: the flat evidence table, all columns or one */

  function renderCellsView(state) {
    var x = state.x;
    var index = state.index;
    var key = state.view.key;
    var cellIdxs = key ? (index.cellsByColumn[key] || []) : range(nrow(x.cells));
    if (!cellIdxs.length) {
      empty(state, "No changed cells.");
      return;
    }
    var rows = cellIdxs.map(function (j) {
      var item = x.cells.item[j] - 1;
      var oldValue = x.cells.old[j];
      var newValue = x.cells.new[j];
      return {
        oldValue: isNull(oldValue) ? null : String(oldValue),
        newValue: isNull(newValue) ? null : String(newValue),
        column: x.cells.column[j],
        strings: [
          String(x.items.snapshot_id[item]), String(x.items.rowid[item]),
          x.cells.column[j], naText(oldValue), naText(newValue)
        ]
      };
    });
    rows = applyFilter(state, rows);
    var page = paginate(state, rows.length);
    var head = [
      h("th", { className: "dlc-num" }, "snapshot"),
      h("th", { className: "dlc-num" }, "rowid"),
      h("th", null, "column"),
      h("th", null, "old"),
      h("th", null, "new")
    ];
    var body = [];
    for (var r = page.start; r < page.end; r++) {
      var row = rows[r];
      var tr = h("tr", null, [
        h("td", { className: "dlc-num" }, row.strings[0]),
        h("td", { className: "dlc-num" }, row.strings[1]),
        h("td", null, row.column)
      ]);
      var oldTd = h("td", { className: colClass(index, row.column) });
      fillCell(oldTd, row.oldValue);
      var newTd = h("td", { className: "dlc-changed " + (colClass(index, row.column) || "") });
      fillCell(newTd, row.newValue);
      tr.appendChild(oldTd);
      tr.appendChild(newTd);
      body.push(tr);
    }
    state.dom.main.appendChild(table(head, body));
    state.dom.main.appendChild(renderPager(state, page));
  }

  /* Schema view: the changes in range, or the whole column roster */

  function snapshotTime(x, index, id) {
    var i = index.snapshotById[String(id)];
    return isNull(i) || isNull(x.snapshots.time[i]) ? "" : x.snapshots.time[i];
  }

  function renderSchemaView(state) {
    var x = state.x;
    var index = state.index;
    var schema = x.schema;
    if (!schema.available) {
      empty(state, "Schema history is not available" + (schema.reason ? ": " + schema.reason : "."));
      return;
    }
    if (state.schemaAll) {
      var rosterHead = [
        h("th", { className: "dlc-marker-col" }, ""),
        h("th", null, "column"),
        h("th", null, "type"),
        h("th", null, "status")
      ];
      var rosterBody = [];
      for (var i = 0; i < nrow(schema.columns); i++) {
        var status = schema.columns.status[i];
        var kind = STATUS_KIND[status];
        rosterBody.push(h("tr", null, [
          h("td", { className: "dlc-marker-col" }, kind ? swatch(kind) : swatch("none")),
          h("td", null, schema.columns.name[i]),
          h("td", null, schema.columns.type[i]),
          h("td", { className: kind ? null : "dlc-na" }, EVENT_LABEL[status] || status)
        ]));
      }
      state.dom.main.appendChild(table(rosterHead, rosterBody));
      return;
    }
    if (!index.schemaChanges.length) {
      empty(state, "No schema changes in this range.");
      return;
    }
    var events = schema.events;
    var rows = index.schemaChanges.map(function (j) {
      var label = EVENT_LABEL[events.kind[j]] || events.kind[j];
      var detail = isNull(events.detail[j]) ? "" : events.detail[j];
      return {
        kind: STATUS_KIND[events.kind[j]],
        strings: [String(events.snapshot_id[j]), snapshotTime(x, index, events.snapshot_id[j]),
          label, events.column_name[j], detail]
      };
    });
    rows = applyFilter(state, rows);
    var page = paginate(state, rows.length);
    var head = [
      h("th", { className: "dlc-marker-col" }, ""),
      h("th", { className: "dlc-num" }, "snapshot"),
      h("th", null, "time (UTC)"),
      h("th", null, "change"),
      h("th", null, "column"),
      h("th", null, "detail")
    ];
    var body = [];
    for (var r = page.start; r < page.end; r++) {
      var row = rows[r];
      body.push(h("tr", null, [
        h("td", { className: "dlc-marker-col" }, swatch(row.kind)),
        h("td", { className: "dlc-num" }, row.strings[0]),
        h("td", null, row.strings[1]),
        h("td", null, row.strings[2]),
        h("td", null, row.strings[3]),
        h("td", null, row.strings[4])
      ]));
    }
    state.dom.main.appendChild(table(head, body));
    state.dom.main.appendChild(renderPager(state, page));
  }

  /* Snapshot card: who, when, why, and what the snapshot did to the schema */

  function renderSnapshotCard(state) {
    var x = state.x;
    var index = state.index;
    var i = index.snapshotById[state.view.key];
    if (isNull(i)) return;
    var s = x.snapshots;
    var meta = [];
    if (!isNull(s.time[i])) meta.push(s.time[i] + " UTC");
    if (!isNull(s.author[i]) && s.author[i] !== "") meta.push(s.author[i]);
    var headline = h("div", null, [
      h("strong", null, "Snapshot #" + s.snapshot_id[i]),
      meta.length ? h("span", { className: "dlc-meta" }, DOT + meta.join(DOT)) : null
    ]);
    var message = (!isNull(s.commit_message[i]) && s.commit_message[i] !== "")
      ? h("div", { className: "dlc-message" }, s.commit_message[i])
      : null;
    var counts = h("div", { className: "dlc-meta" },
      fmtInt(s.inserted[i]) + " inserted" + DOT + fmtInt(s.updated[i]) + " updated" + DOT +
      fmtInt(s.deleted[i]) + " deleted");
    var events = x.schema.events;
    var list = [];
    for (var j = 0; j < nrow(events); j++) {
      if (String(events.snapshot_id[j]) !== state.view.key) continue;
      var detail = isNull(events.detail[j]) ? "" : events.detail[j];
      list.push(h("li", null, (EVENT_LABEL[events.kind[j]] || events.kind[j]) + " " +
        events.column_name[j] + (detail ? " (" + detail + ")" : "")));
    }
    state.dom.main.appendChild(h("div", { className: "dlc-snapshot-card" }, [
      headline, message, counts, list.length ? h("ul", null, list) : null
    ]));
  }
})();
