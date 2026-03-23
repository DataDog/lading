use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, List, ListItem, ListState, Paragraph, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Tabs, Wrap,
    },
};

use crate::app::{App, FormMode, FormRow, ImportMode, PreviewState};
use crate::config::LoadProfileKind;
use crate::variants::{ALL_VARIANTS, variant_meta};

// ---------------------------------------------------------------------------
// Top-level draw
// ---------------------------------------------------------------------------

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();

    // Vertical: tab-bar / content / status-bar
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tab bar
            Constraint::Min(0),    // content
            Constraint::Length(1), // status bar
        ])
        .split(area);

    draw_tab_bar(frame, app, root[0]);
    draw_content(frame, app, root[1]);
    draw_status(frame, app, root[2]);

    // Import overlay drawn on top of everything
    if app.import_mode != ImportMode::Inactive {
        draw_import_overlay(frame, app, area);
    }
}

// ---------------------------------------------------------------------------
// Tab bar
// ---------------------------------------------------------------------------

fn draw_tab_bar(frame: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = vec![Line::from(" Build "), Line::from(" Preview ")];
    let tabs = Tabs::new(titles)
        .select(app.tab)
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(tabs, area);
}

// ---------------------------------------------------------------------------
// Content area
// ---------------------------------------------------------------------------

fn draw_content(frame: &mut Frame, app: &App, area: Rect) {
    if app.template_editor_open {
        draw_template_editor(frame, app, area);
        return;
    }
    if app.tab == 1 {
        draw_preview(frame, app, area);
    } else {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(area);
        draw_left(frame, app, cols[0]);
        draw_right(frame, app, cols[1]);
    }
}

// ---------------------------------------------------------------------------
// Template editor (full content area, shown when template_editor_open)
// ---------------------------------------------------------------------------

fn draw_template_editor(frame: &mut Frame, app: &App, area: Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(area);

    draw_template_editor_text(frame, app, cols[0]);
    draw_template_reference(frame, cols[1]);
}

fn draw_template_editor_text(frame: &mut Frame, app: &App, area: Rect) {
    let inner_area = Block::default()
        .borders(Borders::ALL)
        .title(" Template Editor  (Ctrl+S save   Esc close) ")
        .inner(area);

    let viewport_h = inner_area.height as usize;
    let line_num_w = 4usize; // "NNN " — enough for ~999 lines

    let mut lines: Vec<Line> = Vec::with_capacity(viewport_h);
    for row_idx in app.template_scroll..(app.template_scroll + viewport_h) {
        let line_content = app
            .template_lines
            .get(row_idx)
            .map(|s| s.as_str())
            .unwrap_or("");
        let num_span = Span::styled(
            format!("{:>width$} ", row_idx + 1, width = line_num_w - 1),
            Style::default().fg(Color::DarkGray),
        );

        if row_idx == app.template_cursor_row {
            let col = app.template_cursor_col.min(line_content.len());
            let before = &line_content[..col];
            let cursor_ch = line_content[col..]
                .chars()
                .next()
                .map(|c| c.to_string())
                .unwrap_or_else(|| " ".to_string());
            let after = if col < line_content.len() {
                &line_content[col + cursor_ch.len()..]
            } else {
                ""
            };
            lines.push(Line::from(vec![
                num_span,
                Span::raw(before.to_string()),
                Span::styled(
                    cursor_ch,
                    Style::default().bg(Color::Yellow).fg(Color::Black),
                ),
                Span::raw(after.to_string()),
            ]));
        } else {
            lines.push(Line::from(vec![
                num_span,
                Span::raw(line_content.to_string()),
            ]));
        }
    }

    // Save-path indicator below editor
    let title_suffix = if app.template_just_saved {
        format!(" → {}  ✓ Saved", app.template_path)
    } else if app.template_path.is_empty() {
        " (Ctrl+S to save to /tmp/lading_template.yaml)".to_string()
    } else {
        format!(" → {}", app.template_path)
    };

    let title_style = if app.template_just_saved {
        Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Template Editor{title_suffix} "))
        .title_style(title_style);

    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, area);

    // Error line at the very bottom of the editor if any
    if let Some(err) = &app.error {
        let err_area = Rect {
            x: area.x + 1,
            y: area.y + area.height.saturating_sub(2),
            width: area.width.saturating_sub(2),
            height: 1,
        };
        let err_para = Paragraph::new(Span::styled(
            format!("⚠  {err}"),
            Style::default().fg(Color::Red),
        ));
        frame.render_widget(err_para, err_area);
    }
}

fn draw_template_reference(frame: &mut Frame, area: Rect) {
    let text = "\
 Quick Reference

 !const value
   Fixed JSON literal.
   !const \"text\"
   !const 42 / true / null

 !choose [a, b, c]
   Pick uniformly at random.

 !range {min: N, max: N}
   Random integer (inclusive).

 !weighted
   - weight: 75
     value: !const \"INFO\"
   - weight: 25
     value: !const \"WARN\"

 !format
   template: \"{} took {}ms\"
   args: [!var svc, !range {..}]

 !object
   field: generator
   ...

 !array
   length: {min: 0, max: 5}
   element: generator

 !timestamp
   RFC-3339 UTC, advancing.

 !with
   bind: {x: generator}
   in: uses !var x

 !reference name
   Reuse from definitions:

 definitions:
   name: generator

 !concat [gen1, gen2]
   Merge same-typed results.

 Tip: Tab inserts 2 spaces.
";
    let lines: Vec<Line> = text
        .lines()
        .map(|l| {
            Line::from(Span::styled(
                l.to_string(),
                Style::default().fg(Color::DarkGray),
            ))
        })
        .collect();
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" Reference "))
        .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Build tab — Left panel (form)
// ---------------------------------------------------------------------------

const LABEL_W: usize = 16; // fixed label column width (chars)

fn draw_left(frame: &mut Frame, app: &App, area: Rect) {
    let rows = app.form_rows();
    let (items, highlight_idx) = build_form_list_items(app, &rows);

    // Reserve 2 lines for error display if needed
    let (list_area, error_area) = if app.error.is_some() {
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(2)])
            .split(area);
        (split[0], Some(split[1]))
    } else {
        (area, None)
    };

    let mut state = ListState::default();
    state.select(Some(highlight_idx));

    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(" Build "));
    frame.render_stateful_widget(list, list_area, &mut state);

    if let (Some(err_area), Some(err)) = (error_area, &app.error) {
        let err_para = Paragraph::new(Line::from(Span::styled(
            format!("  ⚠  {err}"),
            Style::default().fg(Color::Red),
        )));
        frame.render_widget(err_para, err_area);
    }
}

/// Build the list items for the form and return the rendered-item index to highlight.
fn build_form_list_items(app: &App, rows: &[FormRow]) -> (Vec<ListItem<'static>>, usize) {
    let mut items: Vec<ListItem<'static>> = Vec::new();
    let mut highlight_idx = 0usize;
    let mut item_idx = 0usize;

    for (ri, &row) in rows.iter().enumerate() {
        let focused = ri == app.form_row;
        let editing = app.form_editing && focused;

        match row {
            FormRow::Variant => {
                // Header row
                let arrow = if app.variant_expanded { "▼" } else { "▶" };
                let label = variant_meta(app.selected_variant()).label;
                let label_str = format!("  {arrow} {:<width$}", "Variant", width = LABEL_W - 4);
                let row_style = if focused && !app.variant_expanded {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(label_str, row_style),
                    Span::styled(label.to_string(), Style::default().fg(Color::Cyan)),
                ])));
                if focused && !app.variant_expanded {
                    highlight_idx = item_idx;
                }
                item_idx += 1;

                // Inline sub-list when expanded
                if app.variant_expanded {
                    for (vi, &vk) in ALL_VARIANTS.iter().enumerate() {
                        let meta = variant_meta(vk);
                        let is_selected = vi == app.variant_cursor;
                        let is_highlighted = vi == app.variant_sub_cursor;
                        let prefix = if is_selected { "     ✓ " } else { "       " };
                        let sub_style = if is_highlighted {
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD)
                        } else if is_selected {
                            Style::default().fg(Color::Green)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        };
                        items.push(ListItem::new(Line::from(Span::styled(
                            format!("{prefix}{}", meta.label),
                            sub_style,
                        ))));
                        if is_highlighted {
                            highlight_idx = item_idx;
                        }
                        item_idx += 1;
                    }
                }
            }

            FormRow::LoadProfile => {
                let arrow = if app.load_profile_expanded {
                    "▼"
                } else {
                    "▶"
                };
                let lp_label = match app.load_profile_kind {
                    LoadProfileKind::Constant => "constant",
                    LoadProfileKind::Linear => "linear",
                };
                let label_str =
                    format!("  {arrow} {:<width$}", "Load Profile", width = LABEL_W - 4);
                let row_style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(label_str, row_style),
                    Span::styled(lp_label.to_string(), Style::default().fg(Color::Cyan)),
                ])));
                if focused && !app.load_profile_expanded {
                    highlight_idx = item_idx;
                }
                item_idx += 1;

                if app.load_profile_expanded {
                    let options = ["constant", "linear"];
                    for (oi, &opt) in options.iter().enumerate() {
                        let is_cur = oi == app.load_profile_sub_cursor;
                        let sub_style = if is_cur {
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(Color::DarkGray)
                        };
                        items.push(ListItem::new(Line::from(Span::styled(
                            format!("       {opt}"),
                            sub_style,
                        ))));
                        if is_cur {
                            highlight_idx = item_idx;
                        }
                        item_idx += 1;
                    }
                }
            }

            FormRow::GeneratorComponent => {
                let arrow = if app.generator_expanded { "▼" } else { "▶" };
                let label_str = format!("  {arrow} {:<width$}", "Generator", width = LABEL_W - 4);
                let row_style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().add_modifier(Modifier::BOLD)
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(label_str, row_style),
                    Span::styled("logrotate_fs", Style::default().fg(Color::DarkGray)),
                ])));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }

            FormRow::BlackholeComponent => {
                let arrow = if app.blackhole_expanded { "▼" } else { "▶" };
                let label_str = format!("  {arrow} {:<width$}", "Blackhole", width = LABEL_W - 4);
                let count_str = match app.blackhole_entries.len() {
                    0 => "none".to_string(),
                    n => format!("{n} entr{}", if n == 1 { "y" } else { "ies" }),
                };
                let row_style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().add_modifier(Modifier::BOLD)
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled(label_str, row_style),
                    Span::styled(count_str, Style::default().fg(Color::DarkGray)),
                ])));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }

            FormRow::BlackholeEntry(i) => {
                let entry = &app.blackhole_entries[i];
                let kind_str = format!("    {:<8}", entry.kind.label());
                let addr_str = if editing {
                    app.input.clone()
                } else {
                    entry.addr.clone()
                };
                let kind_style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                let addr_style = if editing {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Cyan)
                };
                let mut spans = vec![
                    Span::styled(kind_str, kind_style),
                    Span::styled(addr_str, addr_style),
                ];
                if editing {
                    spans.push(Span::styled(
                        "█",
                        Style::default().add_modifier(Modifier::RAPID_BLINK),
                    ));
                }
                items.push(ListItem::new(Line::from(spans)));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }

            FormRow::TemplateEditAction => {
                let path_hint = if app.template_path.is_empty() {
                    " (no path set)".to_string()
                } else {
                    format!(" {}", app.template_path)
                };
                let style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                items.push(ListItem::new(Line::from(vec![
                    Span::styled("    Edit template", style),
                    Span::styled(path_hint, Style::default().fg(Color::DarkGray)),
                ])));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }

            FormRow::BlackholeAddEntry => {
                let style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                };
                items.push(ListItem::new(Line::from(Span::styled(
                    "    + Add entry",
                    style,
                ))));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }

            _ => {
                // Text / toggle rows
                let (label, value) = form_row_display(app, row);
                let label_str = format!("    {label:<width$}", width = LABEL_W);
                let label_style = if focused {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                let value_style = if editing {
                    Style::default().add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::Cyan)
                };
                let mut spans = vec![
                    Span::styled(label_str, label_style),
                    Span::styled(value, value_style),
                ];
                if editing {
                    spans.push(Span::styled(
                        "█",
                        Style::default().add_modifier(Modifier::RAPID_BLINK),
                    ));
                }
                items.push(ListItem::new(Line::from(spans)));
                if focused {
                    highlight_idx = item_idx;
                }
                item_idx += 1;
            }
        }
    }

    (items, highlight_idx)
}

fn form_row_display(app: &App, row: FormRow) -> (&'static str, String) {
    let editing = app.form_editing && {
        let rows = app.form_rows();
        rows.get(app.form_row).copied() == Some(row)
    };
    let value = if editing {
        app.input.clone()
    } else {
        match row {
            FormRow::ConfigPath => app.save_path.clone(),
            FormRow::ConcurrentLogs => app.concurrent_logs.to_string(),
            FormRow::MaxBytesPerLog => app.max_bytes_per_log.clone(),
            FormRow::TotalRotations => app.total_rotations.to_string(),
            FormRow::MaxDepth => app.max_depth.to_string(),
            FormRow::MountPoint => app.mount_point.clone(),
            FormRow::ConstantRate => app.constant_rate.clone(),
            FormRow::LinearInitial => app.linear_initial.clone(),
            FormRow::LinearRate => app.linear_rate.clone(),
            FormRow::MaxPrebuildCache => app.max_prebuild_cache.clone(),
            FormRow::MaxBlockSize => app.max_block_size.clone(),
            FormRow::SplunkHecEncoding => {
                if app.splunk_enc_cursor == 0 {
                    "text".into()
                } else {
                    "json".into()
                }
            }
            FormRow::StaticPath => app.static_path.clone(),
            FormRow::TemplatedJsonPath => app.template_path.clone(),
            FormRow::SavePath => app.save_path.clone(),
            _ => String::new(),
        }
    };
    let label = match row {
        FormRow::ConfigPath => "Config Path",
        FormRow::ConcurrentLogs => "Concurrent",
        FormRow::MaxBytesPerLog => "Max Bytes",
        FormRow::TotalRotations => "Rotations",
        FormRow::MaxDepth => "Max Depth",
        FormRow::MountPoint => "Mount",
        FormRow::ConstantRate => "Rate",
        FormRow::LinearInitial => "Initial",
        FormRow::LinearRate => "Rate/step",
        FormRow::MaxPrebuildCache => "Prebuild",
        FormRow::MaxBlockSize => "Block Size",
        FormRow::SplunkHecEncoding => "Encoding",
        FormRow::StaticPath => "File Path",
        FormRow::TemplatedJsonPath => "Template",
        FormRow::SavePath => "Save Path",
        _ => "",
    };
    (label, value)
}

// ---------------------------------------------------------------------------
// Build tab — Right panel
// ---------------------------------------------------------------------------

fn draw_right(frame: &mut Frame, app: &App, area: Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    draw_form_guidance(frame, app, rows[0]);
    draw_yaml_preview(frame, app, rows[1]);
}

fn draw_form_guidance(frame: &mut Frame, app: &App, area: Rect) {
    let text = form_row_guidance(app);
    let lines: Vec<Line> = text.lines().map(|l| Line::from(format!("  {l}"))).collect();
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(" Guidance "))
        .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

fn form_row_guidance(app: &App) -> String {
    let rows = app.form_rows();
    let row = rows.get(app.form_row).copied();
    match row {
        Some(FormRow::ConfigPath) => "Path to a saved lading YAML config file.\n\
             \n\
             Press Enter to edit the path. If the file\n\
             at that path is a valid lading config, all\n\
             fields will be populated automatically.\n\
             \n\
             Ctrl+S saves the current config to this path."
            .into(),
        Some(FormRow::Variant) if !app.variant_expanded => {
            // Show overview of all variants
            let mut text = "Select the log payload format.\n\
                 Press Enter to open the variant list.\n\
                 \n\
                 Available variants:\n"
                .to_string();
            for &vk in ALL_VARIANTS {
                let m = variant_meta(vk);
                let first = m.description.lines().next().unwrap_or("");
                text.push_str(&format!("\n  {}  —  {}", m.label, first));
            }
            text
        }
        Some(FormRow::Variant) => {
            // Sub-menu open: show details for the currently highlighted variant
            let meta = variant_meta(ALL_VARIANTS[app.variant_sub_cursor]);
            format!(
                "{}\n\
                 \n\
                 Example:\n  {}",
                meta.description, meta.example_line
            )
        }
        Some(FormRow::LoadProfile) if !app.load_profile_expanded => {
            "constant  — emit at a fixed bytes/s rate\n\
             linear    — start slow and ramp up each second\n\
             \n\
             Press Enter to change."
                .into()
        }
        Some(FormRow::LoadProfile) => match app.load_profile_sub_cursor {
            0 => "constant: emit at a steady bytes/s rate\nthroughout the entire test run.".into(),
            _ => "linear: start at the Initial rate and add\nRate/step each second.".into(),
        },
        Some(FormRow::ConcurrentLogs) => "How many log files to write simultaneously.\n\
             Higher values stress the target's file handling\n\
             but increase lading's CPU and memory usage.\n\
             \n\
             Default: 8"
            .into(),
        Some(FormRow::MaxBytesPerLog) => "Maximum size of each log file before it is rotated.\n\
             \n\
             Accepted units: KiB  MiB  GiB\n\
             Examples: 100MiB   1GiB   512KiB\n\
             \n\
             Default: 100MiB"
            .into(),
        Some(FormRow::TotalRotations) => "How many times each log file is rotated (archived)\n\
             before being deleted.\n\
             \n\
             High counts create more files and test rotation\n\
             handling in the target.\n\
             \n\
             Default: 4"
            .into(),
        Some(FormRow::MaxDepth) => "Directory depth for log files below the mount point.\n\
             \n\
             0 = flat: all log files in the root directory.\n\
             1 = one level of subdirectories, etc.\n\
             \n\
             Default: 0"
            .into(),
        Some(FormRow::MountPoint) => "Where the FUSE filesystem is mounted.\n\
             \n\
             The path will be remapped under /tmp if it\n\
             is outside /tmp (no root required).\n\
             \n\
             Default: /tmp/logrotate"
            .into(),
        Some(FormRow::ConstantRate) => "Bytes generated per second (held constant).\n\
             \n\
             Use a byte string:  1MiB   500KiB   10MiB\n\
             \n\
             Default: 1MiB"
            .into(),
        Some(FormRow::LinearInitial) => "Starting rate for linear load growth.\n\
             \n\
             The generator begins at this rate and increases\n\
             by the Rate/step amount each second.\n\
             \n\
             Default: 1MiB"
            .into(),
        Some(FormRow::LinearRate) => "How much to increase the rate each second.\n\
             \n\
             Added to the current rate every second.\n\
             \n\
             Default: 100KiB"
            .into(),
        Some(FormRow::MaxPrebuildCache) => "Maximum size of the pre-built payload cache.\n\
             \n\
             Lading pre-generates payloads to reduce CPU\n\
             overhead during the test. Larger cache means\n\
             more payload variety at higher memory cost.\n\
             \n\
             Default: 1GiB"
            .into(),
        Some(FormRow::MaxBlockSize) => "Maximum size of a single pre-built payload block.\n\
             \n\
             Smaller blocks → finer granularity.\n\
             Larger blocks → less overhead.\n\
             Should be ≤ Max Bytes.\n\
             \n\
             Default: 2MiB"
            .into(),
        Some(FormRow::SplunkHecEncoding) => "Encoding for Splunk HEC event payloads.\n\
             \n\
             text  — raw string in the event field\n\
             json  — structured JSON in the event field\n\
             \n\
             Press Enter to toggle.\n\
             \n\
             Default: json"
            .into(),
        Some(FormRow::StaticPath) => "Path to the static content file.\n\
             \n\
             The file is read once at startup and its\n\
             content is streamed into the log files.\n\
             \n\
             For static_chunks the file is split by\n\
             lines to fill blocks up to Max Block Size."
            .into(),
        Some(FormRow::TemplatedJsonPath) => "Path to the YAML template file.\n\
             \n\
             The template defines the JSON schema and\n\
             value distributions for generated records.\n\
             \n\
             See lading_payload docs for template format."
            .into(),
        Some(FormRow::SavePath) => "Output YAML file path.\n\
             \n\
             Press Enter to write the config file.\n\
             An existing file will be overwritten.\n\
             \n\
             Ctrl+S saves to this path at any time."
            .into(),
        Some(FormRow::TemplateEditAction) => "Opens the template editor for this file.\n\
             \n\
             Build the template YAML directly in the TUI.\n\
             Ctrl+S saves it to the Template Path above.\n\
             Esc returns to the form.\n\
             \n\
             If the path file exists it will be loaded;\n\
             otherwise the default starter template is used.\n\
             \n\
             See the right panel for a tag quick-reference."
            .into(),
        Some(FormRow::GeneratorComponent) => "The generator produces log files using the\n\
             logrotate_fs FUSE filesystem.\n\
             \n\
             Press Enter to expand/collapse settings."
            .into(),
        Some(FormRow::BlackholeComponent) => {
            "Blackhole servers absorb traffic generated by lading.\n\
             Each entry is a server that lading starts and\n\
             listens for connections.\n\
             \n\
             Press Enter to expand/collapse.\n\
             \n\
             Use http for most log shippers.\n\
             Use tcp/udp for raw socket targets."
                .into()
        }
        Some(FormRow::BlackholeEntry(_)) => "← →  change type (http / tcp / udp)\n\
             Enter edit binding address\n\
             d     delete this entry\n\
             \n\
             Use http for most log shippers.\n\
             Use tcp/udp for raw socket targets.\n\
             \n\
             Default address: 127.0.0.1:9091"
            .into(),
        Some(FormRow::BlackholeAddEntry) => "Press Enter to add a new HTTP blackhole entry.\n\
             \n\
             The address is auto-assigned with the next\n\
             available port after the last entry."
            .into(),
        _ => String::new(),
    }
}

fn draw_yaml_preview(frame: &mut Frame, app: &App, area: Rect) {
    let yaml = app.current_yaml();
    let para = Paragraph::new(yaml)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" YAML Preview "),
        )
        .style(Style::default().fg(Color::Cyan))
        .wrap(Wrap { trim: false });
    frame.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Preview tab
// ---------------------------------------------------------------------------

fn draw_preview(frame: &mut Frame, app: &App, area: Rect) {
    match &app.preview_state {
        PreviewState::Idle => draw_preview_idle(frame, app, area),
        PreviewState::Building => draw_preview_building(frame, app, area),
        PreviewState::Running => draw_preview_running(frame, app, area),
        PreviewState::Failed(msg) => draw_preview_failed(frame, app, area, msg.clone()),
        PreviewState::Stopped => draw_preview_stopped(frame, app, area),
    }
}

fn preview_cols(area: Rect) -> (Rect, Rect) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);
    (cols[0], cols[1])
}

fn draw_preview_idle(frame: &mut Frame, app: &App, area: Rect) {
    let (left, right) = preview_cols(area);

    // Left: config path input
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(left);

    let display = Line::from(vec![
        Span::styled(
            app.preview_config_input.clone(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled("█", Style::default().add_modifier(Modifier::RAPID_BLINK)),
    ]);
    let input_para = Paragraph::new(display).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Config Path "),
    );
    frame.render_widget(input_para, rows[0]);

    let mut info_lines: Vec<Line> = vec![
        Line::from(Span::styled(
            "  r / Enter  run lading",
            Style::default().fg(Color::Yellow),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  i          import config into Build tab",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  Tab        switch to Build tab",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    if let Some(err) = &app.preview_config_error {
        info_lines.push(Line::from(""));
        info_lines.push(Line::from(Span::styled(
            format!("  ⚠  {err}"),
            Style::default().fg(Color::Red),
        )));
    }
    if app.dirty {
        info_lines.push(Line::from(""));
        info_lines.push(Line::from(Span::styled(
            "  ⚠  unsaved changes in Build tab",
            Style::default().fg(Color::Yellow),
        )));
    }
    let info_para = Paragraph::new(info_lines)
        .block(Block::default().borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(info_para, rows[1]);

    // Right: instructions
    let right_lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Enter a saved YAML config path and",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  press r or Enter to run.",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  lading will be built from source",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  (cargo build -p lading --features logrotate_fs)",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  Press i to import a config into the",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  Build tab for editing.",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    let right_para = Paragraph::new(right_lines)
        .block(Block::default().borders(Borders::ALL).title(" Preview "))
        .wrap(Wrap { trim: false });
    frame.render_widget(right_para, right);
}

fn draw_preview_building(frame: &mut Frame, app: &App, area: Rect) {
    let (left, right) = preview_cols(area);

    let left_lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Building lading...",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  This may take a minute on first run.",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  q  cancel",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    let left_para = Paragraph::new(left_lines)
        .block(Block::default().borders(Borders::ALL).title(" Build "))
        .wrap(Wrap { trim: false });
    frame.render_widget(left_para, left);

    // Right: streaming build output (last N lines)
    let log = &app.preview_build_log;
    let lines_vec: Vec<&str> = log.lines().collect();
    let start = lines_vec.len().saturating_sub(100);
    let display: Vec<Line> = lines_vec[start..]
        .iter()
        .map(|&l| Line::from(format!("  {l}")))
        .collect();
    let right_para = Paragraph::new(display)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Build Output "),
        )
        .style(Style::default().fg(Color::DarkGray))
        .wrap(Wrap { trim: false });
    frame.render_widget(right_para, right);
}

/// Appends a collapsible "▶/▼ Config" section to a left-panel line list.
fn append_config_panel(lines: &mut Vec<Line>, app: &App) {
    let arrow = if app.preview_config_panel_expanded {
        "▼"
    } else {
        "▶"
    };
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        format!("  {arrow} Config"),
        Style::default().fg(Color::DarkGray),
    )));
    if app.preview_config_panel_expanded {
        for l in app.preview_config_yaml.lines() {
            lines.push(Line::from(Span::styled(
                format!("    {l}"),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }
}

fn draw_preview_running(frame: &mut Frame, app: &App, area: Rect) {
    let (left, right) = preview_cols(area);

    // Left: status info
    let secs = app.preview_last_refresh.elapsed().as_secs();
    let file_count = app.preview_log_files.len();
    let selected_name = app
        .preview_log_files
        .get(app.preview_file_tab)
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("—");
    let selected_path = app
        .preview_log_files
        .get(app.preview_file_tab)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "—".into());

    let rotated_count = app.preview_rotated_files.len();
    let rotated_arrow = if app.preview_rotated_expanded {
        "▼"
    } else {
        "▶"
    };
    let rotated_label = if rotated_count == 0 {
        format!("  {rotated_arrow} Rotated  none")
    } else {
        format!(
            "  {rotated_arrow} Rotated  {rotated_count} file{}",
            if rotated_count == 1 { "" } else { "s" }
        )
    };

    let mut left_lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Mount:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                app.preview_mount_point.clone(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Files:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                file_count.to_string(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Refresh: ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{secs}s ago"), Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  File:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                selected_name.to_string(),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Path:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(selected_path, Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            rotated_label,
            if rotated_count > 0 {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default().fg(Color::DarkGray)
            },
        )),
    ];
    if app.preview_rotated_expanded {
        for (i, path) in app.preview_rotated_files.iter().enumerate() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            let is_cur = app.preview_viewing_rotated && i == app.preview_rotated_cursor;
            left_lines.push(Line::from(Span::styled(
                format!("    {} {name}", if is_cur { "▶" } else { " " }),
                if is_cur {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            )));
        }
    }
    append_config_panel(&mut left_lines, app);
    left_lines.extend([
        Line::from(""),
        Line::from(Span::styled(
            "  ← →  switch file",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  e    toggle rotated",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  [/]  navigate rotated",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  c    toggle config",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  q    stop lading",
            Style::default().fg(Color::DarkGray),
        )),
    ]);
    let left_para = Paragraph::new(left_lines)
        .block(Block::default().borders(Borders::ALL).title(" Running "))
        .wrap(Wrap { trim: false });
    frame.render_widget(left_para, left);

    draw_file_tabs_panel(frame, app, right);
}

fn draw_file_tabs_panel(frame: &mut Frame, app: &App, area: Rect) {
    if app.preview_log_files.is_empty() {
        let waiting = Paragraph::new("\n  Waiting for log files to appear...")
            .block(Block::default().borders(Borders::ALL).title(" Log Files "))
            .style(Style::default().fg(Color::DarkGray));
        frame.render_widget(waiting, area);
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    // Tab bar for log files
    let tab_titles: Vec<Line> = app
        .preview_log_files
        .iter()
        .map(|p| {
            let name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("?")
                .to_string();
            Line::from(format!(" {name} "))
        })
        .collect();
    let file_tabs = Tabs::new(tab_titles)
        .select(app.preview_file_tab)
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(file_tabs, rows[0]);

    // File content — use snapshot when stopped, live otherwise
    let content_lines: Vec<Line> = app
        .displayed_content()
        .lines()
        .map(|l| Line::from(format!("  {l}")))
        .collect();
    let displayed_lines = content_lines.len() as u16;
    let total_lines = app.displayed_total_lines();
    // rows[1] height minus top/bottom borders
    let visible = rows[1].height.saturating_sub(2);
    let max_scroll = displayed_lines.saturating_sub(visible);
    app.preview_max_scroll.set(max_scroll);
    // Convert lines-from-bottom to lines-from-top
    let scroll_top = max_scroll.saturating_sub(app.preview_content_scroll);
    let total_bytes = app.displayed_total_bytes();
    let bytes_str = format_bytes(total_bytes);
    let content_title = match app.preview_snapshot_idx {
        Some(idx) => {
            let age = app
                .preview_snapshots
                .get(idx)
                .map(|(t, _, _, _)| format!("{}s ago", t.elapsed().as_secs()))
                .unwrap_or_else(|| "?".into());
            format!(
                " Snapshot {}/{} — {} — {total_lines} lines  {bytes_str} ",
                idx + 1,
                app.preview_snapshots.len(),
                age
            )
        }
        None => {
            if app.preview_content_scroll > 0 {
                format!(
                    " {total_lines} lines  {bytes_str}  [↑ scrolled {}] ",
                    app.preview_content_scroll
                )
            } else {
                format!(" {total_lines} lines  {bytes_str}  [tail] ")
            }
        }
    };
    let content_para = Paragraph::new(content_lines)
        .block(Block::default().borders(Borders::ALL).title(content_title))
        .style(Style::default().fg(Color::Cyan))
        .scroll((scroll_top, 0));
    frame.render_widget(content_para, rows[1]);

    // Scrollbar overlay on the right edge
    let mut scrollbar_state = ScrollbarState::new(displayed_lines.saturating_sub(visible) as usize)
        .position(scroll_top as usize);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓")),
        rows[1],
        &mut scrollbar_state,
    );

    // Timeline bar
    draw_snapshot_timeline(frame, app, rows[2]);
}

fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;
    const GIB: usize = 1024 * MIB;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

const MAX_SNAPSHOTS: usize = 120;
const BAR_WIDTH: usize = 30;

fn draw_snapshot_timeline(frame: &mut Frame, app: &App, area: Rect) {
    let line = if matches!(app.preview_state, PreviewState::Running) {
        let n = app.preview_snapshots.len();
        let next_in = 5u64.saturating_sub(app.preview_last_refresh.elapsed().as_secs());

        // Progress bar: how many slots of MAX_SNAPSHOTS have been captured
        let filled = (n * BAR_WIDTH) / MAX_SNAPSHOTS;
        // Animate the leading edge based on countdown
        let pending_char = match next_in {
            0 | 1 => "▒",
            _ => "░",
        };
        let bar: String =
            "█".repeat(filled) + pending_char + &"░".repeat(BAR_WIDTH.saturating_sub(filled + 1));

        let elapsed_secs = n as u64 * 5;
        let elapsed_str = format_duration(elapsed_secs);

        Line::from(vec![
            Span::styled("  [", Style::default().fg(Color::DarkGray)),
            Span::styled(bar, Style::default().fg(Color::Green)),
            Span::styled(
                format!("]  {n}/{MAX_SNAPSHOTS}  {elapsed_str} captured   next in {next_in}s"),
                Style::default().fg(Color::DarkGray),
            ),
        ])
    } else if app.preview_snapshots.is_empty() {
        Line::from(Span::styled(
            "  (no snapshots — run lading to capture)",
            Style::default().fg(Color::DarkGray),
        ))
    } else {
        // Stopped/Failed: navigable bar with cursor
        let n = app.preview_snapshots.len();
        let active = app.preview_snapshot_idx.unwrap_or(n.saturating_sub(1));

        // Cursor position within the bar
        let cursor_pos = if n > 1 {
            (active * BAR_WIDTH) / (n - 1)
        } else {
            BAR_WIDTH
        };
        let bar: String = (0..=BAR_WIDTH)
            .map(|i| if i == cursor_pos { '█' } else { '░' })
            .collect();

        let age_str = app
            .preview_snapshots
            .get(active)
            .map(|(t, _, _, _)| format!("{}s ago", t.elapsed().as_secs()))
            .unwrap_or_default();

        let mut spans = vec![
            Span::styled("  [", Style::default().fg(Color::DarkGray)),
            Span::styled(bar, Style::default().fg(Color::Yellow)),
            Span::styled(
                format!("]  {}/{n}  {age_str}", active + 1),
                Style::default().fg(Color::Yellow),
            ),
        ];
        if active > 0 {
            spans.push(Span::styled(
                "   [ back",
                Style::default().fg(Color::DarkGray),
            ));
        }
        if active + 1 < n {
            spans.push(Span::styled(
                "   ] forward",
                Style::default().fg(Color::DarkGray),
            ));
        }
        Line::from(spans)
    };

    frame.render_widget(Paragraph::new(line), area);
}

fn format_duration(secs: u64) -> String {
    if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

fn draw_preview_failed(frame: &mut Frame, app: &App, area: Rect, msg: String) {
    let (left, right) = preview_cols(area);

    let left_lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Failed.",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  r  retry",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  q  quit",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  Tab  back to Build",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    let left_para = Paragraph::new(left_lines)
        .block(Block::default().borders(Borders::ALL).title(" Error "))
        .wrap(Wrap { trim: false });
    frame.render_widget(left_para, left);

    let err_lines: Vec<Line> = msg
        .lines()
        .map(|l| {
            Line::from(Span::styled(
                format!("  {l}"),
                Style::default().fg(Color::Red),
            ))
        })
        .collect();
    let build_log_lines: Vec<Line> = app
        .preview_build_log
        .lines()
        .rev()
        .take(50)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|l| Line::from(format!("  {l}")))
        .collect();
    let mut all_lines = err_lines;
    if !all_lines.is_empty() && !build_log_lines.is_empty() {
        all_lines.push(Line::from(""));
    }
    all_lines.extend(build_log_lines);
    let right_para = Paragraph::new(all_lines)
        .block(Block::default().borders(Borders::ALL).title(" Details "))
        .style(Style::default().fg(Color::DarkGray))
        .wrap(Wrap { trim: false });
    frame.render_widget(right_para, right);
}

fn draw_preview_stopped(frame: &mut Frame, app: &App, area: Rect) {
    let (left, right) = preview_cols(area);

    let snapshot_count = app.preview_snapshots.len();
    let rotated_count = app.preview_rotated_files.len();
    let rotated_arrow = if app.preview_rotated_expanded {
        "▼"
    } else {
        "▶"
    };
    let rotated_label = if rotated_count == 0 {
        format!("  {rotated_arrow} Rotated  none")
    } else {
        format!(
            "  {rotated_arrow} Rotated  {rotated_count} file{}",
            if rotated_count == 1 { "" } else { "s" }
        )
    };

    let mut left_lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "  Stopped.",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            format!(
                "  {snapshot_count} snapshot{} captured",
                if snapshot_count == 1 { "" } else { "s" }
            ),
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            rotated_label,
            Style::default().fg(Color::DarkGray),
        )),
    ];
    if app.preview_rotated_expanded {
        for (i, path) in app.preview_rotated_files.iter().enumerate() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            let is_cur = app.preview_viewing_rotated && i == app.preview_rotated_cursor;
            left_lines.push(Line::from(Span::styled(
                format!("    {} {name}", if is_cur { "▶" } else { " " }),
                if is_cur {
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            )));
        }
    }
    append_config_panel(&mut left_lines, app);
    left_lines.extend([
        Line::from(""),
        Line::from(Span::styled(
            "  [  snapshots / rotated back",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  ]  snapshots / rotated fwd",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  e  toggle rotated files",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  c  toggle config",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  ↑↓ scroll content",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "  r  run again",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  Tab  back to Build",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "  q  quit",
            Style::default().fg(Color::DarkGray),
        )),
    ]);
    let left_para = Paragraph::new(left_lines)
        .block(Block::default().borders(Borders::ALL).title(" Stopped "))
        .wrap(Wrap { trim: false });
    frame.render_widget(left_para, left);

    // Right: file tabs + content + timeline (same widget as Running)
    draw_file_tabs_panel(frame, app, right);
}

// ---------------------------------------------------------------------------
// Import overlay
// ---------------------------------------------------------------------------

fn draw_import_overlay(frame: &mut Frame, app: &App, area: Rect) {
    // Center a 60x10 box
    let height = 10_u16;
    let width = area.width.min(70);
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let overlay_area = Rect {
        x,
        y,
        width,
        height,
    };

    frame.render_widget(Clear, overlay_area);

    match app.import_mode {
        ImportMode::EnterPath => {
            let inner = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0)])
                .margin(1)
                .split(overlay_area);

            let display = Line::from(vec![
                Span::styled(
                    app.import_input.clone(),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::styled("█", Style::default().add_modifier(Modifier::RAPID_BLINK)),
            ]);
            let input_para = Paragraph::new(display)
                .block(Block::default().borders(Borders::ALL).title(" Path "));
            frame.render_widget(input_para, inner[0]);

            let mut hint_lines = vec![Line::from(Span::styled(
                "  Enter confirm   Esc cancel",
                Style::default().fg(Color::DarkGray),
            ))];
            if let Some(err) = &app.import_error {
                hint_lines.push(Line::from(Span::styled(
                    format!("  ⚠  {err}"),
                    Style::default().fg(Color::Red),
                )));
            }
            let hint_para = Paragraph::new(hint_lines).wrap(Wrap { trim: false });
            frame.render_widget(hint_para, inner[1]);

            let block = Block::default()
                .borders(Borders::ALL)
                .title(" Import Config ")
                .style(Style::default().fg(Color::Yellow));
            frame.render_widget(block, overlay_area);
        }
        ImportMode::ConfirmUnsaved => {
            let inner = Block::default()
                .borders(Borders::ALL)
                .title(" Unsaved Changes ")
                .style(Style::default().fg(Color::Yellow));
            let inner_area = inner.inner(overlay_area);
            frame.render_widget(inner, overlay_area);

            let lines = vec![
                Line::from(""),
                Line::from(Span::styled(
                    "  You have unsaved changes in the Build tab.",
                    Style::default().fg(Color::Yellow),
                )),
                Line::from(""),
                Line::from("  Save them before importing?"),
                Line::from(""),
                Line::from(Span::styled(
                    "  Enter / y  save then import",
                    Style::default().fg(Color::DarkGray),
                )),
                Line::from(Span::styled(
                    "  n          discard and import",
                    Style::default().fg(Color::DarkGray),
                )),
                Line::from(Span::styled(
                    "  Esc        cancel",
                    Style::default().fg(Color::DarkGray),
                )),
            ];
            let para = Paragraph::new(lines).wrap(Wrap { trim: false });
            frame.render_widget(para, inner_area);
        }
        ImportMode::Inactive => {}
    }
}

// ---------------------------------------------------------------------------
// Status bar
// ---------------------------------------------------------------------------

fn draw_status(frame: &mut Frame, app: &App, area: Rect) {
    // Save notification takes priority
    if let Some(_) = app.save_notification {
        let msg = format!(" ✓ Config saved to {}", app.save_path);
        let para = Paragraph::new(msg).style(Style::default().fg(Color::Green));
        frame.render_widget(para, area);
        return;
    }

    let text = if app.template_editor_open {
        " ↑↓←→ navigate   Enter newline   Backspace/Del edit   Tab indent   Ctrl+S save   Esc close"
    } else if app.import_mode != ImportMode::Inactive {
        " Enter confirm   Esc cancel"
    } else if app.tab == 1 {
        match &app.preview_state {
            PreviewState::Idle => {
                " Type config path   r/Enter run (10 min)   i import into Build   Tab build tab   q quit"
            }
            PreviewState::Building => " q cancel build",
            PreviewState::Running => {
                " ↑↓ scroll   t/b top/bottom   ← → switch file   e rotated   [/] navigate rotated   q stop"
            }
            PreviewState::Stopped => {
                " ↑↓ scroll   t/b top/bottom   [/] snapshots or rotated   ←→ switch file/rotated   e rotated   r run again   q quit"
            }
            PreviewState::Failed(_) => " r retry   Tab build tab   q quit",
        }
    } else {
        let rows = app.form_rows();
        let current_row = rows.get(app.form_row).copied();
        match (app.form_mode(), current_row) {
            (FormMode::Navigate, Some(FormRow::BlackholeEntry(_))) => {
                " ← → kind   Enter edit addr   d delete   ↑↓ navigate   ^S save   q quit"
            }
            (FormMode::Navigate, _) => {
                " ↑↓ navigate   Enter edit/expand   i import   Tab preview   r re-seed   ^S save   q quit"
            }
            (FormMode::VariantSubMenu, _) => " ↑↓ select variant   Enter confirm   Esc cancel",
            (FormMode::LoadProfileSubMenu, _) => " ↑↓ select profile   Enter confirm   Esc cancel",
            (FormMode::Editing, _) => " Type to edit   Enter confirm   Esc cancel",
        }
    };
    let para = Paragraph::new(text).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(para, area);
}
