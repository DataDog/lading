use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyModifiers};
use rand::{RngCore, SeedableRng, rngs::SmallRng};

const DEFAULT_TEMPLATE: &str = "\
generator:\n\
  !object\n\
    timestamp: !timestamp\n\
    level:\n\
      !weighted\n\
        - weight: 75\n\
          value: !const \"INFO\"\n\
        - weight: 20\n\
          value: !const \"WARN\"\n\
        - weight: 5\n\
          value: !const \"ERROR\"\n\
    message: !choose [\"request processed\", \"user login\", \"cache miss\", \"db query\"]\n\
    duration_ms: !range { min: 1, max: 5000 }\n\
";

/// Generate 32 bytes of OS entropy for the config seed.
fn fresh_seed() -> [u8; 32] {
    let mut buf = [0u8; 32];
    SmallRng::from_os_rng().fill_bytes(&mut buf);
    buf
}

use crate::config::{
    BlackholeEntry, BlackholeKind, ImportedFields, LoadProfileKind, build_load_profile_value,
    build_variant_value, build_yaml, parse_config,
};
use crate::variants::{ALL_VARIANTS, VariantKind};

// ---------------------------------------------------------------------------
// Form types (replaces old Step/FieldId/SubField wizard)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FormRow {
    ConfigPath,
    // --- component headers ---
    GeneratorComponent,
    BlackholeComponent,
    // --- generator sub-rows (visible when generator_expanded) ---
    Variant,
    ConcurrentLogs,
    MaxBytesPerLog,
    TotalRotations,
    MaxDepth,
    MountPoint,
    LoadProfile,
    ConstantRate,  // only when LoadProfileKind::Constant
    LinearInitial, // only when LoadProfileKind::Linear
    LinearRate,    // only when LoadProfileKind::Linear
    MaxPrebuildCache,
    MaxBlockSize,
    SplunkHecEncoding,  // only when variant == SplunkHec
    StaticPath,         // only when variant == Static or StaticChunks
    TemplatedJsonPath,  // only when variant == TemplatedJson
    TemplateEditAction, // "Open template editor" button row
    // --- blackhole sub-rows (visible when blackhole_expanded) ---
    BlackholeEntry(usize), // one row per entry
    BlackholeAddEntry,     // "+ Add entry" row
    // --- save ---
    SavePath,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FormMode {
    Navigate,
    VariantSubMenu,
    LoadProfileSubMenu,
    Editing,
}

#[derive(Clone, PartialEq, Eq)]
pub enum PreviewState {
    Idle,
    Building,
    Running,
    Failed(String),
    Stopped,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ImportMode {
    Inactive,
    EnterPath,
    ConfirmUnsaved,
}

// ---------------------------------------------------------------------------
// Per-file live state
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LogFileState {
    pub content: String,    // rolling 2000-line display buffer (live tail)
    pub total_lines: usize, // actual line count (may exceed buffer)
    pub total_bytes: usize, // actual byte count of the file
    pub read_pos: u64,      // last byte offset read (incremental reads)
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

pub struct App {
    // --- form navigation ---
    pub form_row: usize,
    pub generator_expanded: bool,
    pub blackhole_expanded: bool,
    pub variant_expanded: bool,
    pub variant_sub_cursor: usize,
    pub load_profile_expanded: bool,
    pub load_profile_sub_cursor: usize, // 0=Constant, 1=Linear
    pub form_editing: bool,
    pub auto_import_done: bool,

    // --- blackhole config ---
    pub blackhole_entries: Vec<BlackholeEntry>,

    // --- config values ---
    pub seed: [u8; 32],
    pub variant_cursor: usize,
    pub concurrent_logs: u16,
    pub max_bytes_per_log: String,
    pub total_rotations: u8,
    pub max_depth: u8,
    pub mount_point: String,
    pub load_profile_kind: LoadProfileKind,
    pub constant_rate: String,
    pub linear_initial: String,
    pub linear_rate: String,
    pub max_prebuild_cache: String,
    pub max_block_size: String,
    pub splunk_enc_cursor: usize, // 0 = text, 1 = json
    pub static_path: String,
    pub template_path: String,

    // --- save state ---
    pub save_path: String,
    pub saved: bool,
    pub dirty: bool,
    pub save_notification: Option<Instant>, // set on save, cleared after ~3s

    // --- transient UI state ---
    pub input: String,
    pub error: Option<String>,
    pub tab: usize, // 0 = Build, 1 = Preview
    pub quit: bool,

    // --- import overlay ---
    pub import_mode: ImportMode,
    pub import_input: String,
    pub import_error: Option<String>,
    pub import_pending_path: String,

    // --- preview tab ---
    pub preview_state: PreviewState,
    pub preview_config_input: String,
    pub preview_config_error: Option<String>,
    pub preview_build_child: Option<Child>,
    pub preview_lading_child: Option<Child>,
    pub preview_build_log: String,
    pub preview_build_log_rx: Option<Receiver<String>>,
    pub preview_lading_log: String,
    pub preview_lading_log_rx: Option<Receiver<String>>,
    pub preview_log_files: Vec<PathBuf>, // active .log files
    pub preview_file_tab: usize,
    pub preview_log_states: HashMap<PathBuf, LogFileState>, // per-file live state + snapshots
    pub preview_rotated_files: Vec<Vec<PathBuf>>,           // [tab_idx] -> sorted rotated files
    pub preview_rotated_cache: HashMap<PathBuf, (String, usize, usize)>, // path -> (content, lines, bytes)
    pub preview_rotated_expanded: bool,
    pub preview_rotated_cursor: usize, // index within current tab's rotated list
    pub preview_viewing_rotated: bool, // if true, show rotated content instead
    pub preview_full_file_content: HashMap<PathBuf, (String, usize, usize)>, // captured on stop: full file (content, lines, bytes)
    pub preview_last_refresh: Instant,      // 1s lightweight tick (incremental reads)
    pub preview_last_full_refresh: Instant, // 5s full refresh (dir walk + rotated cache)
    pub preview_mount_point: String,
    pub preview_run_config: String, // patched copy of config with remapped mount_point
    pub preview_config_yaml: String, // YAML content of the active run config
    pub preview_config_panel_expanded: bool, // whether config panel is expanded in left panel
    pub preview_content_scroll: u16, // lines scrolled up from bottom (0 = follow tail)
    pub preview_max_scroll: std::cell::Cell<u16>, // set by render each frame; max useful scroll value

    // --- template editor ---
    pub template_editor_open: bool,
    pub template_lines: Vec<String>,
    pub template_cursor_row: usize,
    pub template_cursor_col: usize,
    pub template_scroll: usize,
    pub template_just_saved: bool,
}

impl App {
    pub fn new(config_path: Option<String>) -> Self {
        let seed = fresh_seed();

        let mut app = App {
            form_row: 0,
            generator_expanded: true,
            blackhole_expanded: true,
            variant_expanded: false,
            variant_sub_cursor: 0,
            load_profile_expanded: false,
            load_profile_sub_cursor: 0,
            form_editing: false,
            auto_import_done: false,
            blackhole_entries: vec![BlackholeEntry {
                kind: BlackholeKind::Http,
                addr: "127.0.0.1:9091".into(),
            }],
            seed,
            variant_cursor: 0,
            concurrent_logs: 8,
            max_bytes_per_log: "100MiB".into(),
            total_rotations: 4,
            max_depth: 0,
            mount_point: "/tmp/logrotate".into(),
            load_profile_kind: LoadProfileKind::Constant,
            constant_rate: "1MiB".into(),
            linear_initial: "1MiB".into(),
            linear_rate: "100KiB".into(),
            max_prebuild_cache: "1GiB".into(),
            max_block_size: "2MiB".into(),
            splunk_enc_cursor: 0,
            static_path: String::new(),
            template_path: String::new(),
            save_path: config_path.unwrap_or_else(|| "/tmp/lading_config.yaml".into()),
            saved: false,
            dirty: false,
            save_notification: None,
            input: String::new(),
            error: None,
            tab: 0,
            quit: false,
            import_mode: ImportMode::Inactive,
            import_input: String::new(),
            import_error: None,
            import_pending_path: String::new(),
            preview_state: PreviewState::Idle,
            preview_config_input: "/tmp/lading_config.yaml".into(),
            preview_config_error: None,
            preview_build_child: None,
            preview_lading_child: None,
            preview_build_log: String::new(),
            preview_build_log_rx: None,
            preview_lading_log: String::new(),
            preview_lading_log_rx: None,
            preview_log_files: Vec::new(),
            preview_file_tab: 0,
            preview_log_states: HashMap::new(),
            preview_rotated_files: Vec::new(),
            preview_rotated_cache: HashMap::new(),
            preview_rotated_expanded: false,
            preview_rotated_cursor: 0,
            preview_viewing_rotated: false,
            preview_full_file_content: HashMap::new(),
            preview_last_refresh: Instant::now(),
            preview_last_full_refresh: Instant::now(),
            preview_mount_point: String::new(),
            preview_run_config: String::new(),
            preview_config_yaml: String::new(),
            preview_config_panel_expanded: false,
            preview_content_scroll: 0,
            preview_max_scroll: std::cell::Cell::new(0),
            template_editor_open: false,
            template_lines: DEFAULT_TEMPLATE.lines().map(str::to_string).collect(),
            template_cursor_row: 0,
            template_cursor_col: 0,
            template_scroll: 0,
            template_just_saved: false,
        };
        app.try_auto_import();
        app
    }

    pub fn selected_variant(&self) -> VariantKind {
        ALL_VARIANTS[self.variant_cursor]
    }

    pub fn current_yaml(&self) -> String {
        let variant = build_variant_value(
            self.selected_variant(),
            self.splunk_enc_cursor,
            &self.static_path,
            &self.template_path,
        );
        let lp = build_load_profile_value(
            self.load_profile_kind,
            &self.constant_rate,
            &self.linear_initial,
            &self.linear_rate,
        );
        build_yaml(
            &self.seed,
            self.concurrent_logs,
            &self.max_bytes_per_log,
            self.total_rotations,
            self.max_depth,
            &self.mount_point,
            &self.max_prebuild_cache,
            &self.max_block_size,
            variant,
            lp,
            &self.blackhole_entries,
        )
    }

    /// Returns the path of the currently selected rotated file, if any.
    pub fn current_rotated_path(&self) -> Option<&PathBuf> {
        self.preview_rotated_files
            .get(self.preview_file_tab)
            .and_then(|v| v.get(self.preview_rotated_cursor))
    }

    /// Returns the content to display: rotated file > full stopped > live rolling.
    pub fn displayed_content(&self) -> &str {
        if self.preview_viewing_rotated {
            if let Some(path) = self.current_rotated_path() {
                if let Some((content, _, _)) = self.preview_rotated_cache.get(path) {
                    return content;
                }
            }
            return "";
        }
        let path = self.preview_log_files.get(self.preview_file_tab);
        if self.preview_state == PreviewState::Stopped {
            if let Some(p) = path {
                if let Some((content, _, _)) = self.preview_full_file_content.get(p) {
                    return content;
                }
            }
        }
        let state = path.and_then(|p| self.preview_log_states.get(p));
        state.map(|s| s.content.as_str()).unwrap_or("")
    }

    /// Returns the total line count for the currently displayed content.
    pub fn displayed_total_lines(&self) -> usize {
        if self.preview_viewing_rotated {
            if let Some(path) = self.current_rotated_path() {
                if let Some((_, lines, _)) = self.preview_rotated_cache.get(path) {
                    return *lines;
                }
            }
            return 0;
        }
        let path = self.preview_log_files.get(self.preview_file_tab);
        if self.preview_state == PreviewState::Stopped {
            if let Some(p) = path {
                if let Some((_, lines, _)) = self.preview_full_file_content.get(p) {
                    return *lines;
                }
            }
        }
        path.and_then(|p| self.preview_log_states.get(p))
            .map(|s| s.total_lines)
            .unwrap_or(0)
    }

    /// Returns the total byte count for the currently displayed content.
    pub fn displayed_total_bytes(&self) -> usize {
        if self.preview_viewing_rotated {
            if let Some(path) = self.current_rotated_path() {
                if let Some((_, _, bytes)) = self.preview_rotated_cache.get(path) {
                    return *bytes;
                }
            }
            return 0;
        }
        let path = self.preview_log_files.get(self.preview_file_tab);
        if self.preview_state == PreviewState::Stopped {
            if let Some(p) = path {
                if let Some((_, _, bytes)) = self.preview_full_file_content.get(p) {
                    return *bytes;
                }
            }
        }
        path.and_then(|p| self.preview_log_states.get(p))
            .map(|s| s.total_bytes)
            .unwrap_or(0)
    }

    // -----------------------------------------------------------------------
    // Tick — called every loop iteration for background work
    // -----------------------------------------------------------------------

    pub fn tick(&mut self) {
        // Clear save notification after 3 seconds
        if let Some(t) = self.save_notification {
            if t.elapsed().as_secs() >= 3 {
                self.save_notification = None;
            }
        }
        match &self.preview_state {
            PreviewState::Building => self.tick_building(),
            PreviewState::Running => self.tick_running(),
            _ => {}
        }
    }

    fn tick_building(&mut self) {
        // Drain lines from the background reader thread (non-blocking)
        if let Some(rx) = &self.preview_build_log_rx {
            while let Ok(line) = rx.try_recv() {
                self.preview_build_log.push_str(&line);
                self.preview_build_log.push('\n');
            }
        }

        let exited = self
            .preview_build_child
            .as_mut()
            .and_then(|c| c.try_wait().ok())
            .flatten();
        if let Some(status) = exited {
            if status.success() {
                self.preview_build_child = None;
                self.start_lading();
            } else {
                let log = self.preview_build_log.clone();
                let last = log
                    .lines()
                    .rev()
                    .take(5)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>()
                    .join("\n");
                self.preview_state = PreviewState::Failed(format!("Build failed:\n{last}"));
                self.preview_build_child = None;
            }
        }
    }

    fn tick_running(&mut self) {
        // Drain lading log lines from the background reader thread (non-blocking)
        if let Some(rx) = &self.preview_lading_log_rx {
            while let Ok(line) = rx.try_recv() {
                self.preview_lading_log.push_str(&line);
                self.preview_lading_log.push('\n');
            }
        }

        let exited = self
            .preview_lading_child
            .as_mut()
            .and_then(|c| c.try_wait().ok())
            .flatten();
        if let Some(status) = exited {
            self.preview_lading_child = None;
            self.preview_state =
                PreviewState::Failed(format!("lading exited unexpectedly: {status}"));
            return;
        }

        if self.preview_last_refresh.elapsed() >= Duration::from_secs(1) {
            self.refresh_log_files_lightweight();
        }
        if self.preview_last_full_refresh.elapsed() >= Duration::from_secs(5) {
            self.refresh_log_files_full();
        }
    }

    // 1s tick: incremental reads of active files.
    fn refresh_log_files_lightweight(&mut self) {
        if self.preview_mount_point.is_empty() {
            return;
        }
        let active_paths: Vec<PathBuf> = self.preview_log_files.clone();
        for path in &active_paths {
            self.read_file_incremental(path);
        }
        self.preview_last_refresh = Instant::now();
    }

    // 5s tick: dir walk, file grouping, rotated cache update.
    fn refresh_log_files_full(&mut self) {
        if self.preview_mount_point.is_empty() {
            return;
        }
        let mount = std::path::Path::new(&self.preview_mount_point);
        let mut active: Vec<PathBuf> = Vec::new();
        let mut all_rotated: Vec<PathBuf> = Vec::new();
        if let Ok(entries) = self.walk_dir(mount) {
            for path in entries {
                if path.extension().and_then(|e| e.to_str()) == Some("log") {
                    active.push(path);
                } else if is_rotated_log(&path) {
                    all_rotated.push(path);
                }
            }
        }
        active.sort();
        all_rotated.sort();

        // Group rotated files by their active-file parent (foo.log.1 → foo.log).
        let mut rotated: Vec<Vec<PathBuf>> = vec![vec![]; active.len()];
        for rpath in all_rotated {
            if let Some(idx) = active.iter().position(|a| is_rotated_of(a, &rpath)) {
                rotated[idx].push(rpath);
            }
        }

        self.preview_log_files = active;
        self.preview_rotated_files = rotated;

        // clamp tab index
        if !self.preview_log_files.is_empty() {
            self.preview_file_tab = self.preview_file_tab.min(self.preview_log_files.len() - 1);
        } else {
            self.preview_file_tab = 0;
        }
        // clamp rotated cursor to current tab's list
        let cur_rot_len = self
            .preview_rotated_files
            .get(self.preview_file_tab)
            .map(Vec::len)
            .unwrap_or(0);
        if cur_rot_len == 0 {
            self.preview_rotated_cursor = 0;
            if self.preview_viewing_rotated {
                self.preview_viewing_rotated = false;
            }
        } else {
            self.preview_rotated_cursor = self.preview_rotated_cursor.min(cur_rot_len - 1);
        }

        // Drop state for files that have been rotated out of the active list.
        let active_paths = self.preview_log_files.clone();
        self.preview_log_states
            .retain(|p, _| active_paths.contains(p));

        self.preview_last_full_refresh = Instant::now();
    }

    // Called by stop_preview to flush everything before the FUSE mount dies.
    fn refresh_log_files(&mut self) {
        self.refresh_log_files_full();
        self.refresh_log_files_lightweight();
    }

    fn read_file_incremental(&mut self, path: &PathBuf) {
        let Ok(mut file) = std::fs::File::open(path) else {
            return;
        };
        let Ok(meta) = file.metadata() else { return };
        let new_size = meta.len();
        let state = self.preview_log_states.entry(path.clone()).or_default();

        if new_size == state.read_pos {
            return; // nothing new
        }

        if new_size < state.read_pos {
            // File was truncated/rotated — reset and re-read from the start.
            state.read_pos = 0;
            state.content.clear();
            state.total_lines = 0;
        } else {
            file.seek(SeekFrom::Start(state.read_pos)).ok();
        }

        let mut new_bytes = Vec::new();
        file.read_to_end(&mut new_bytes).ok();
        state.total_bytes = new_size as usize;
        state.read_pos = new_size;

        let new_text = String::from_utf8_lossy(&new_bytes);
        let new_line_count = new_text.lines().count();
        let mut lines: Vec<&str> = state.content.lines().collect();
        lines.extend(new_text.lines());
        state.total_lines = state.total_lines.saturating_add(new_line_count);
        let start = lines.len().saturating_sub(2000);
        state.content = lines[start..].join("\n");
    }

    pub fn load_rotated_file(&mut self) {
        let path = self
            .preview_rotated_files
            .get(self.preview_file_tab)
            .and_then(|v| v.get(self.preview_rotated_cursor))
            .cloned();
        let Some(path) = path else { return };
        // Attempt a read in case the eager cache missed this file (e.g. it appeared
        // after the last refresh). After FUSE unmounts this will silently fail and
        // we fall back to whatever is already in the cache.
        if let Ok(content) = std::fs::read_to_string(&path) {
            let lines = content.lines().count();
            let bytes = content.len();
            self.preview_rotated_cache.insert(path, (content, lines, bytes));
        }
        self.preview_viewing_rotated = true;
        self.preview_content_scroll = 0;
    }

    fn walk_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        let mut result = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                result.extend(self.walk_dir(&path)?);
            } else {
                result.push(path);
            }
        }
        Ok(result)
    }

    fn start_preview(&mut self) {
        let config_path = self.preview_config_input.trim().to_string();
        if config_path.is_empty() {
            self.preview_config_error = Some("Config path cannot be empty".into());
            return;
        }

        // Read and validate config
        let yaml = match std::fs::read_to_string(&config_path) {
            Err(e) => {
                self.preview_config_error = Some(format!("Cannot read config: {e}"));
                return;
            }
            Ok(y) => y,
        };

        // Require a blackhole section — lading needs a sink for generated traffic
        if !config_has_blackhole(&yaml) {
            self.preview_config_error = Some(
                "Config has no 'blackhole' section — add one before running.\n\
                 Example:\n\
                 blackhole:\n\
                 \x20 - http:\n\
                 \x20     binding_addr: \"127.0.0.1:9091\""
                    .into(),
            );
            return;
        }

        // Remap mount_point into /tmp so we don't need root or pre-created dirs
        let original_mount =
            extract_mount_point(&yaml).unwrap_or_else(|| "/tmp/logrotate".to_string());
        let effective_mount = remap_to_tmp(&original_mount);
        if let Err(e) = std::fs::create_dir_all(&effective_mount) {
            self.preview_config_error =
                Some(format!("Cannot create mount dir {effective_mount}: {e}"));
            return;
        }
        self.preview_mount_point = effective_mount.clone();

        // Write a patched config to a temp file so the original is untouched
        let patched_yaml = patch_mount_point(&yaml, &effective_mount);
        let run_config = "/tmp/lading_tui_run.yaml".to_string();
        if let Err(e) = std::fs::write(&run_config, &patched_yaml) {
            self.preview_config_error = Some(format!("Cannot write run config: {e}"));
            return;
        }
        self.preview_run_config = run_config;
        self.preview_config_yaml = patched_yaml;
        self.preview_config_panel_expanded = false;

        self.preview_config_error = None;
        self.preview_build_log = String::new();
        self.preview_build_log_rx = None;
        self.preview_lading_log = String::new();
        self.preview_lading_log_rx = None;
        self.preview_log_files = Vec::new();
        self.preview_log_states.clear();
        self.preview_file_tab = 0;
        self.preview_rotated_files = Vec::new();
        self.preview_rotated_cache.clear();
        self.preview_rotated_expanded = false;
        self.preview_rotated_cursor = 0;
        self.preview_viewing_rotated = false;
        self.preview_full_file_content.clear();
        self.preview_last_refresh = Instant::now();
        self.preview_last_full_refresh = Instant::now();

        match Command::new("cargo")
            .args(["build", "-p", "lading", "--features", "logrotate_fs"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(mut child) => {
                // Spawn a background thread to drain stderr without blocking the UI
                if let Some(stderr) = child.stderr.take() {
                    let (tx, rx) = mpsc::channel();
                    std::thread::spawn(move || {
                        let reader = BufReader::new(stderr);
                        for line in reader.lines().flatten() {
                            if tx.send(line).is_err() {
                                break;
                            }
                        }
                    });
                    self.preview_build_log_rx = Some(rx);
                }
                self.preview_build_child = Some(child);
                self.preview_state = PreviewState::Building;
            }
            Err(e) => {
                self.preview_state = PreviewState::Failed(format!("Failed to spawn cargo: {e}"));
            }
        }
    }

    fn start_lading(&mut self) {
        let config_path = self.preview_run_config.clone();
        match Command::new("target/debug/lading")
            .args([
                "--config-path",
                &config_path,
                "--no-target",
                "--experiment-duration-seconds",
                "600",
                "--capture-path",
                "/tmp/lading_tui_capture.jsonl",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => {
                self.preview_lading_child = Some(child);
                self.preview_state = PreviewState::Running;
                self.refresh_log_files();
            }
            Err(e) => {
                self.preview_state = PreviewState::Failed(format!("Failed to spawn lading: {e}"));
            }
        }
    }

    pub fn stop_preview(&mut self) {
        // Do a final refresh before killing lading so all rotated file content
        // is cached while the FUSE mount is still live.
        self.refresh_log_files();

        // Capture full contents of all active log files while FUSE is still mounted.
        // The rolling 2000-line buffer in LogFileState is a live-tail view; on stop
        // we want the complete file so the user can scroll to the real first line.
        let active_paths: Vec<PathBuf> = self.preview_log_files.clone();
        for path in &active_paths {
            if let Ok(content) = std::fs::read_to_string(path) {
                let lines = content.lines().count();
                let bytes = content.len();
                self.preview_full_file_content.insert(path.clone(), (content, lines, bytes));
            }
        }

        // Cache all rotated files now, while FUSE is still mounted.
        // We do this at stop time rather than eagerly every 5s to avoid blocking
        // the main thread with potentially GBs of I/O during the run.
        let all_rotated: Vec<PathBuf> = self.preview_rotated_files
            .iter()
            .flatten()
            .cloned()
            .collect();
        for path in all_rotated {
            if let Ok(content) = std::fs::read_to_string(&path) {
                let lines = content.lines().count();
                let bytes = content.len();
                self.preview_rotated_cache.insert(path, (content, lines, bytes));
            }
        }

        if let Some(mut child) = self.preview_lading_child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(mut child) = self.preview_build_child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.preview_build_log_rx = None;
        self.preview_lading_log_rx = None;
        self.preview_state = PreviewState::Stopped;
    }

    // -----------------------------------------------------------------------
    // Import
    // -----------------------------------------------------------------------

    fn try_import(&mut self) {
        let path = self.import_input.trim().to_string();
        if path.is_empty() {
            self.import_error = Some("Path cannot be empty".into());
            return;
        }
        let yaml = match std::fs::read_to_string(&path) {
            Ok(y) => y,
            Err(e) => {
                self.import_error = Some(format!("Cannot read file: {e}"));
                return;
            }
        };
        match parse_config(&yaml) {
            Err(e) => {
                self.import_error = Some(e);
            }
            Ok(fields) => {
                if self.dirty {
                    self.import_pending_path = path;
                    self.import_mode = ImportMode::ConfirmUnsaved;
                } else {
                    self.apply_import(fields);
                    self.import_mode = ImportMode::Inactive;
                    // sync preview config input with whatever was imported
                    self.preview_config_input = path;
                }
            }
        }
    }

    pub fn apply_import(&mut self, fields: ImportedFields) {
        if let Some(s) = fields.seed {
            self.seed = s;
        }
        if let Some(v) = fields.concurrent_logs {
            self.concurrent_logs = v;
        }
        if let Some(v) = fields.max_bytes_per_log {
            self.max_bytes_per_log = v;
        }
        if let Some(v) = fields.total_rotations {
            self.total_rotations = v;
        }
        if let Some(v) = fields.max_depth {
            self.max_depth = v;
        }
        if let Some(v) = fields.mount_point {
            self.mount_point = v;
        }
        if let Some(kind) = fields.variant_kind {
            self.variant_cursor = ALL_VARIANTS.iter().position(|&k| k == kind).unwrap_or(0);
        }
        if let Some(v) = fields.splunk_enc {
            self.splunk_enc_cursor = v;
        }
        if let Some(v) = fields.static_path {
            self.static_path = v;
        }
        if let Some(v) = fields.template_path {
            self.template_path = v;
        }
        if let Some(v) = fields.load_profile_kind {
            self.load_profile_kind = v;
        }
        if let Some(v) = fields.constant_rate {
            self.constant_rate = v;
        }
        if let Some(v) = fields.linear_initial {
            self.linear_initial = v;
        }
        if let Some(v) = fields.linear_rate {
            self.linear_rate = v;
        }
        if let Some(v) = fields.max_prebuild_cache {
            self.max_prebuild_cache = v;
        }
        if let Some(v) = fields.max_block_size {
            self.max_block_size = v;
        }
        if let Some(entries) = fields.blackhole_entries {
            self.blackhole_entries = entries;
        }
        self.dirty = false;
        self.import_mode = ImportMode::Inactive;
        self.import_error = None;
    }

    // -----------------------------------------------------------------------
    // Key handling
    // -----------------------------------------------------------------------

    pub fn handle_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        // Import overlay consumes all keys
        if self.import_mode != ImportMode::Inactive {
            self.handle_import_key(code);
            return;
        }

        // Template editor consumes all keys when open
        if self.template_editor_open {
            self.handle_template_editor_key(code, modifiers);
            return;
        }

        // Ctrl+S → fast-save (Build tab only, any form mode)
        if code == KeyCode::Char('s') && modifiers.contains(KeyModifiers::CONTROL) {
            if self.tab == 0 {
                self.do_save_to_path();
            }
            return;
        }

        // 'i' → open import overlay (blocked when typing in a text field)
        if code == KeyCode::Char('i') && self.tab == 0 && !self.form_editing {
            self.import_mode = ImportMode::EnterPath;
            self.import_input = String::new();
            self.import_error = None;
            return;
        }

        if self.tab == 1 {
            self.handle_preview_key(code);
            return;
        }

        self.handle_form_key(code);
    }

    fn handle_import_key(&mut self, code: KeyCode) {
        match self.import_mode {
            ImportMode::EnterPath => match code {
                KeyCode::Char(c) => {
                    self.import_input.push(c);
                    self.import_error = None;
                }
                KeyCode::Backspace => {
                    self.import_input.pop();
                    self.import_error = None;
                }
                KeyCode::Enter => self.try_import(),
                KeyCode::Esc => self.import_mode = ImportMode::Inactive,
                _ => {}
            },
            ImportMode::ConfirmUnsaved => match code {
                KeyCode::Enter | KeyCode::Char('y') => {
                    // Save first, then import
                    let yaml = self.current_yaml();
                    if let Err(e) = std::fs::write(&self.save_path, &yaml) {
                        self.import_error = Some(format!("Save failed: {e}"));
                        self.import_mode = ImportMode::EnterPath;
                        return;
                    }
                    self.saved = true;
                    self.save_notification = Some(Instant::now());
                    let path = self.import_pending_path.clone();
                    let yaml = match std::fs::read_to_string(&path) {
                        Ok(y) => y,
                        Err(e) => {
                            self.import_error = Some(format!("Cannot read file: {e}"));
                            self.import_mode = ImportMode::Inactive;
                            return;
                        }
                    };
                    match parse_config(&yaml) {
                        Ok(fields) => {
                            self.preview_config_input = path.clone();
                            self.apply_import(fields);
                        }
                        Err(e) => {
                            self.import_error = Some(e);
                            self.import_mode = ImportMode::Inactive;
                        }
                    }
                }
                KeyCode::Char('n') => {
                    let path = self.import_pending_path.clone();
                    let yaml = match std::fs::read_to_string(&path) {
                        Ok(y) => y,
                        Err(e) => {
                            self.import_error = Some(format!("Cannot read file: {e}"));
                            self.import_mode = ImportMode::Inactive;
                            return;
                        }
                    };
                    match parse_config(&yaml) {
                        Ok(fields) => {
                            self.preview_config_input = path.clone();
                            self.apply_import(fields);
                        }
                        Err(e) => {
                            self.import_error = Some(e);
                            self.import_mode = ImportMode::Inactive;
                        }
                    }
                }
                KeyCode::Esc => {
                    self.import_mode = ImportMode::Inactive;
                }
                _ => {}
            },
            ImportMode::Inactive => {}
        }
    }

    fn handle_preview_key(&mut self, code: KeyCode) {
        match &self.preview_state.clone() {
            PreviewState::Idle => match code {
                KeyCode::Enter | KeyCode::Char('r') => {
                    self.start_preview();
                }
                KeyCode::Char('q') => self.quit = true,
                KeyCode::Char(c) => {
                    self.preview_config_input.push(c);
                    self.preview_config_error = None;
                }
                KeyCode::Backspace => {
                    self.preview_config_input.pop();
                    self.preview_config_error = None;
                }
                KeyCode::Tab => self.tab = 0,
                _ => {}
            },
            PreviewState::Building => match code {
                KeyCode::Char('q') => self.stop_preview(),
                _ => {}
            },
            PreviewState::Running => match code {
                KeyCode::Up => {
                    self.preview_content_scroll = self.preview_content_scroll.saturating_add(3);
                }
                KeyCode::Down => {
                    self.preview_content_scroll = self.preview_content_scroll.saturating_sub(3);
                }
                KeyCode::Left => {
                    if self.preview_rotated_expanded {
                        // Navigate between rotated files for this tab
                        if self.preview_rotated_cursor > 0 {
                            self.preview_rotated_cursor -= 1;
                            self.load_rotated_file();
                        }
                    } else if self.preview_file_tab > 0 {
                        self.preview_file_tab -= 1;
                        self.preview_content_scroll = 0;
                        self.preview_viewing_rotated = false;
                        self.preview_rotated_cursor = 0;
                    }
                }
                KeyCode::Right => {
                    if self.preview_rotated_expanded {
                        let cur_rot_len = self
                            .preview_rotated_files
                            .get(self.preview_file_tab)
                            .map(Vec::len)
                            .unwrap_or(0);
                        if self.preview_rotated_cursor + 1 < cur_rot_len {
                            self.preview_rotated_cursor += 1;
                            self.load_rotated_file();
                        }
                    } else if !self.preview_log_files.is_empty()
                        && self.preview_file_tab + 1 < self.preview_log_files.len()
                    {
                        self.preview_file_tab += 1;
                        self.preview_content_scroll = 0;
                        self.preview_viewing_rotated = false;
                        self.preview_rotated_cursor = 0;
                    }
                }
                KeyCode::Char('e') => {
                    self.preview_rotated_expanded = !self.preview_rotated_expanded;
                    if self.preview_rotated_expanded {
                        self.preview_rotated_cursor = 0;
                        self.load_rotated_file();
                    } else {
                        self.preview_viewing_rotated = false;
                    }
                }
                KeyCode::Char('t') => {
                    self.preview_content_scroll = self.preview_max_scroll.get();
                }
                KeyCode::Char('b') => {
                    self.preview_content_scroll = 0;
                }
                KeyCode::Char('c') => {
                    self.preview_config_panel_expanded = !self.preview_config_panel_expanded;
                }
                KeyCode::Char('q') => self.stop_preview(),
                _ => {}
            },
            PreviewState::Failed(_) | PreviewState::Stopped => match code {
                KeyCode::Left => {
                    if self.preview_rotated_expanded {
                        if self.preview_rotated_cursor > 0 {
                            self.preview_rotated_cursor -= 1;
                            self.load_rotated_file();
                        }
                    } else if self.preview_file_tab > 0 {
                        self.preview_file_tab -= 1;
                        self.preview_content_scroll = 0;
                        self.preview_viewing_rotated = false;
                        self.preview_rotated_cursor = 0;
                    }
                }
                KeyCode::Right => {
                    if self.preview_rotated_expanded {
                        let cur_rot_len = self
                            .preview_rotated_files
                            .get(self.preview_file_tab)
                            .map(Vec::len)
                            .unwrap_or(0);
                        if self.preview_rotated_cursor + 1 < cur_rot_len {
                            self.preview_rotated_cursor += 1;
                            self.load_rotated_file();
                        }
                    } else if !self.preview_log_files.is_empty()
                        && self.preview_file_tab + 1 < self.preview_log_files.len()
                    {
                        self.preview_file_tab += 1;
                        self.preview_content_scroll = 0;
                        self.preview_viewing_rotated = false;
                        self.preview_rotated_cursor = 0;
                    }
                }
                KeyCode::Char('e') => {
                    self.preview_rotated_expanded = !self.preview_rotated_expanded;
                    if self.preview_rotated_expanded {
                        self.preview_rotated_cursor = 0;
                        self.load_rotated_file();
                    } else {
                        self.preview_viewing_rotated = false;
                    }
                }
                KeyCode::Up => {
                    self.preview_content_scroll = self.preview_content_scroll.saturating_add(3);
                }
                KeyCode::Down => {
                    self.preview_content_scroll = self.preview_content_scroll.saturating_sub(3);
                }
                KeyCode::Char('t') => {
                    self.preview_content_scroll = self.preview_max_scroll.get();
                }
                KeyCode::Char('b') => {
                    self.preview_content_scroll = 0;
                }
                KeyCode::Char('c') => {
                    self.preview_config_panel_expanded = !self.preview_config_panel_expanded;
                }
                KeyCode::Char('r') => {
                    self.preview_state = PreviewState::Idle;
                }
                KeyCode::Char('q') => self.quit = true,
                KeyCode::Tab => self.tab = 0,
                _ => {}
            },
        }
    }

    fn handle_form_key(&mut self, code: KeyCode) {
        match self.form_mode() {
            FormMode::Navigate => self.handle_form_navigate(code),
            FormMode::VariantSubMenu => self.handle_variant_submenu(code),
            FormMode::LoadProfileSubMenu => self.handle_load_profile_submenu(code),
            FormMode::Editing => self.handle_form_editing(code),
        }
    }

    fn handle_form_navigate(&mut self, code: KeyCode) {
        let rows = self.form_rows();
        match code {
            KeyCode::Up => {
                if self.form_row > 0 {
                    self.form_row -= 1;
                }
            }
            KeyCode::Down => {
                if self.form_row + 1 < rows.len() {
                    self.form_row += 1;
                }
            }
            KeyCode::Enter => {
                let row = rows[self.form_row];
                match row {
                    FormRow::Variant => {
                        self.variant_sub_cursor = self.variant_cursor;
                        self.variant_expanded = true;
                    }
                    FormRow::LoadProfile => {
                        self.load_profile_sub_cursor = match self.load_profile_kind {
                            LoadProfileKind::Constant => 0,
                            LoadProfileKind::Linear => 1,
                        };
                        self.load_profile_expanded = true;
                    }
                    FormRow::SplunkHecEncoding => {
                        self.splunk_enc_cursor ^= 1;
                        self.dirty = true;
                    }
                    FormRow::GeneratorComponent => {
                        self.generator_expanded = !self.generator_expanded;
                        self.clamp_form_row();
                    }
                    FormRow::BlackholeComponent => {
                        self.blackhole_expanded = !self.blackhole_expanded;
                        self.clamp_form_row();
                    }
                    FormRow::TemplateEditAction => {
                        // Load from file if path is set and file exists; otherwise keep current lines
                        if !self.template_path.is_empty() {
                            if let Ok(content) = std::fs::read_to_string(&self.template_path) {
                                self.template_lines = content.lines().map(str::to_string).collect();
                                if self.template_lines.is_empty() {
                                    self.template_lines = vec![String::new()];
                                }
                            }
                        }
                        self.template_cursor_row = 0;
                        self.template_cursor_col = 0;
                        self.template_scroll = 0;
                        self.template_editor_open = true;
                    }
                    FormRow::BlackholeEntry(_) => self.begin_edit(),
                    FormRow::BlackholeAddEntry => {
                        let addr = next_blackhole_addr(&self.blackhole_entries);
                        self.blackhole_entries.push(BlackholeEntry {
                            kind: BlackholeKind::Http,
                            addr,
                        });
                        self.dirty = true;
                    }
                    _ => self.begin_edit(),
                }
            }
            KeyCode::Left => {
                if let Some(FormRow::BlackholeEntry(i)) = rows.get(self.form_row).copied() {
                    if i < self.blackhole_entries.len() {
                        self.blackhole_entries[i].kind = self.blackhole_entries[i].kind.prev();
                        self.dirty = true;
                    }
                }
            }
            KeyCode::Right => {
                if let Some(FormRow::BlackholeEntry(i)) = rows.get(self.form_row).copied() {
                    if i < self.blackhole_entries.len() {
                        self.blackhole_entries[i].kind = self.blackhole_entries[i].kind.next();
                        self.dirty = true;
                    }
                }
            }
            KeyCode::Char('d') => {
                if let Some(FormRow::BlackholeEntry(i)) = rows.get(self.form_row).copied() {
                    self.blackhole_entries.remove(i);
                    self.dirty = true;
                    self.clamp_form_row();
                }
            }
            KeyCode::Tab => self.tab = 1,
            KeyCode::Char('r') => self.seed = fresh_seed(),
            KeyCode::Char('q') => self.quit = true,
            _ => {}
        }
    }

    fn handle_variant_submenu(&mut self, code: KeyCode) {
        match code {
            KeyCode::Up => {
                if self.variant_sub_cursor > 0 {
                    self.variant_sub_cursor -= 1;
                }
            }
            KeyCode::Down => {
                if self.variant_sub_cursor + 1 < ALL_VARIANTS.len() {
                    self.variant_sub_cursor += 1;
                }
            }
            KeyCode::Enter => {
                self.variant_cursor = self.variant_sub_cursor;
                self.variant_expanded = false;
                self.dirty = true;
                self.clamp_form_row();
            }
            KeyCode::Esc => {
                self.variant_expanded = false;
            }
            _ => {}
        }
    }

    fn handle_load_profile_submenu(&mut self, code: KeyCode) {
        match code {
            KeyCode::Up | KeyCode::Down => {
                self.load_profile_sub_cursor = 1 - self.load_profile_sub_cursor;
            }
            KeyCode::Enter => {
                self.load_profile_kind = match self.load_profile_sub_cursor {
                    0 => LoadProfileKind::Constant,
                    _ => LoadProfileKind::Linear,
                };
                self.load_profile_expanded = false;
                self.dirty = true;
                self.clamp_form_row();
            }
            KeyCode::Esc => {
                self.load_profile_expanded = false;
            }
            _ => {}
        }
    }

    fn handle_template_editor_key(&mut self, code: KeyCode, modifiers: KeyModifiers) {
        // Any key clears the saved indicator
        self.template_just_saved = false;

        // Ctrl+S → save template to file
        if code == KeyCode::Char('s') && modifiers.contains(KeyModifiers::CONTROL) {
            self.save_template();
            return;
        }
        match code {
            KeyCode::Esc => {
                self.template_editor_open = false;
                self.error = None;
            }
            KeyCode::Up => {
                if self.template_cursor_row > 0 {
                    self.template_cursor_row -= 1;
                    let len = self.template_lines[self.template_cursor_row].len();
                    self.template_cursor_col = self.template_cursor_col.min(len);
                }
            }
            KeyCode::Down => {
                if self.template_cursor_row + 1 < self.template_lines.len() {
                    self.template_cursor_row += 1;
                    let len = self.template_lines[self.template_cursor_row].len();
                    self.template_cursor_col = self.template_cursor_col.min(len);
                }
            }
            KeyCode::Left => {
                if self.template_cursor_col > 0 {
                    self.template_cursor_col -= 1;
                } else if self.template_cursor_row > 0 {
                    self.template_cursor_row -= 1;
                    self.template_cursor_col = self.template_lines[self.template_cursor_row].len();
                }
            }
            KeyCode::Right => {
                let len = self.template_lines[self.template_cursor_row].len();
                if self.template_cursor_col < len {
                    self.template_cursor_col += 1;
                } else if self.template_cursor_row + 1 < self.template_lines.len() {
                    self.template_cursor_row += 1;
                    self.template_cursor_col = 0;
                }
            }
            KeyCode::Home => {
                self.template_cursor_col = 0;
            }
            KeyCode::End => {
                self.template_cursor_col = self.template_lines[self.template_cursor_row].len();
            }
            KeyCode::Tab => {
                // Insert 2 spaces for YAML-friendly indentation
                let row = self.template_cursor_row;
                let col = self.template_cursor_col;
                self.template_lines[row].insert_str(col, "  ");
                self.template_cursor_col += 2;
            }
            KeyCode::Char(c) => {
                let row = self.template_cursor_row;
                let col = self.template_cursor_col;
                self.template_lines[row].insert(col, c);
                self.template_cursor_col += 1;
            }
            KeyCode::Enter => {
                let row = self.template_cursor_row;
                let col = self.template_cursor_col;
                let rest = self.template_lines[row].split_off(col);
                self.template_lines.insert(row + 1, rest);
                self.template_cursor_row += 1;
                self.template_cursor_col = 0;
            }
            KeyCode::Backspace => {
                let row = self.template_cursor_row;
                let col = self.template_cursor_col;
                if col > 0 {
                    self.template_lines[row].remove(col - 1);
                    self.template_cursor_col -= 1;
                } else if row > 0 {
                    let line = self.template_lines.remove(row);
                    self.template_cursor_row -= 1;
                    let prev_len = self.template_lines[self.template_cursor_row].len();
                    self.template_lines[self.template_cursor_row].push_str(&line);
                    self.template_cursor_col = prev_len;
                }
            }
            KeyCode::Delete => {
                let row = self.template_cursor_row;
                let col = self.template_cursor_col;
                if col < self.template_lines[row].len() {
                    self.template_lines[row].remove(col);
                } else if row + 1 < self.template_lines.len() {
                    let next = self.template_lines.remove(row + 1);
                    self.template_lines[row].push_str(&next);
                }
            }
            _ => {}
        }
        self.template_scroll_to_cursor();
    }

    fn save_template(&mut self) {
        if self.template_path.is_empty() {
            self.template_path = "/tmp/lading_template.yaml".to_string();
        }
        let content = self.template_lines.join("\n");
        match std::fs::write(&self.template_path, content) {
            Ok(()) => {
                self.error = None;
                self.template_just_saved = true;
            }
            Err(e) => {
                self.error = Some(format!("Save failed: {e}"));
            }
        }
    }

    fn template_scroll_to_cursor(&mut self) {
        const VIEWPORT: usize = 28; // conservative estimate; actual height varies
        if self.template_cursor_row < self.template_scroll {
            self.template_scroll = self.template_cursor_row;
        } else if self.template_cursor_row >= self.template_scroll + VIEWPORT {
            self.template_scroll = self.template_cursor_row.saturating_sub(VIEWPORT - 1);
        }
    }

    fn handle_form_editing(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char(c) => {
                self.input.push(c);
                self.error = None;
            }
            KeyCode::Backspace => {
                self.input.pop();
                self.error = None;
            }
            KeyCode::Enter => self.commit_edit(),
            KeyCode::Esc => {
                self.form_editing = false;
                self.error = None;
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // Form helpers
    // -----------------------------------------------------------------------

    /// Ordered list of visible form rows based on current config state.
    pub fn form_rows(&self) -> Vec<FormRow> {
        let mut rows = vec![FormRow::ConfigPath, FormRow::GeneratorComponent];

        if self.generator_expanded {
            rows.push(FormRow::Variant);
            // Variant-specific sub-rows appear immediately after Variant
            match self.selected_variant() {
                VariantKind::SplunkHec => rows.push(FormRow::SplunkHecEncoding),
                VariantKind::Static | VariantKind::StaticChunks => rows.push(FormRow::StaticPath),
                VariantKind::TemplatedJson => {
                    rows.push(FormRow::TemplatedJsonPath);
                    rows.push(FormRow::TemplateEditAction);
                }
                _ => {}
            }
            rows.push(FormRow::ConcurrentLogs);
            rows.push(FormRow::MaxBytesPerLog);
            rows.push(FormRow::TotalRotations);
            rows.push(FormRow::MaxDepth);
            rows.push(FormRow::MountPoint);
            rows.push(FormRow::LoadProfile);
            match self.load_profile_kind {
                LoadProfileKind::Constant => rows.push(FormRow::ConstantRate),
                LoadProfileKind::Linear => {
                    rows.push(FormRow::LinearInitial);
                    rows.push(FormRow::LinearRate);
                }
            }
            rows.push(FormRow::MaxPrebuildCache);
            rows.push(FormRow::MaxBlockSize);
        }

        rows.push(FormRow::BlackholeComponent);

        if self.blackhole_expanded {
            for i in 0..self.blackhole_entries.len() {
                rows.push(FormRow::BlackholeEntry(i));
            }
            rows.push(FormRow::BlackholeAddEntry);
        }

        rows.push(FormRow::SavePath);
        rows
    }

    pub fn form_mode(&self) -> FormMode {
        if self.variant_expanded {
            FormMode::VariantSubMenu
        } else if self.load_profile_expanded {
            FormMode::LoadProfileSubMenu
        } else if self.form_editing {
            FormMode::Editing
        } else {
            FormMode::Navigate
        }
    }

    fn clamp_form_row(&mut self) {
        let max = self.form_rows().len().saturating_sub(1);
        self.form_row = self.form_row.min(max);
    }

    fn begin_edit(&mut self) {
        let rows = self.form_rows();
        let row = rows[self.form_row];
        self.input = match row {
            FormRow::ConfigPath => self.save_path.clone(),
            FormRow::ConcurrentLogs => self.concurrent_logs.to_string(),
            FormRow::MaxBytesPerLog => self.max_bytes_per_log.clone(),
            FormRow::TotalRotations => self.total_rotations.to_string(),
            FormRow::MaxDepth => self.max_depth.to_string(),
            FormRow::MountPoint => self.mount_point.clone(),
            FormRow::ConstantRate => self.constant_rate.clone(),
            FormRow::LinearInitial => self.linear_initial.clone(),
            FormRow::LinearRate => self.linear_rate.clone(),
            FormRow::MaxPrebuildCache => self.max_prebuild_cache.clone(),
            FormRow::MaxBlockSize => self.max_block_size.clone(),
            FormRow::StaticPath => self.static_path.clone(),
            FormRow::TemplatedJsonPath => self.template_path.clone(),
            FormRow::SavePath => self.save_path.clone(),
            FormRow::BlackholeEntry(i) => {
                if i < self.blackhole_entries.len() {
                    self.blackhole_entries[i].addr.clone()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        };
        self.form_editing = true;
        self.error = None;
    }

    fn commit_edit(&mut self) {
        let rows = self.form_rows();
        let row = rows[self.form_row];
        match row {
            FormRow::ConfigPath => {
                let path = self.input.trim().to_string();
                if path.is_empty() {
                    self.form_editing = false;
                    return;
                }
                // Try to auto-import the file at the new path
                if let Ok(yaml) = std::fs::read_to_string(&path) {
                    if let Ok(fields) = parse_config(&yaml) {
                        self.apply_import(fields);
                        self.save_path = path.clone();
                        self.preview_config_input = path;
                        self.form_editing = false;
                        return;
                    }
                }
                // Not a valid lading config — just update the path
                self.save_path = path.clone();
                self.preview_config_input = path;
                self.form_editing = false;
            }
            FormRow::ConcurrentLogs => match self.input.trim().parse::<u16>() {
                Ok(v) if v > 0 => {
                    self.concurrent_logs = v;
                    self.dirty = true;
                    self.form_editing = false;
                }
                _ => self.error = Some("Must be a positive integer (1–65535)".into()),
            },
            FormRow::MaxBytesPerLog => {
                if self.input.trim().is_empty() {
                    self.error = Some("Cannot be empty — e.g. 100MiB".into());
                } else {
                    self.max_bytes_per_log = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::TotalRotations => match self.input.trim().parse::<u8>() {
                Ok(v) => {
                    self.total_rotations = v;
                    self.dirty = true;
                    self.form_editing = false;
                }
                _ => self.error = Some("Must be 0–255".into()),
            },
            FormRow::MaxDepth => match self.input.trim().parse::<u8>() {
                Ok(v) => {
                    self.max_depth = v;
                    self.dirty = true;
                    self.form_editing = false;
                }
                _ => self.error = Some("Must be 0–255".into()),
            },
            FormRow::MountPoint => {
                if self.input.trim().is_empty() {
                    self.error = Some("Path cannot be empty".into());
                } else {
                    self.mount_point = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::ConstantRate => {
                if self.input.trim().is_empty() {
                    self.error = Some("Rate cannot be empty — e.g. 1MiB".into());
                } else {
                    self.constant_rate = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::LinearInitial => {
                if self.input.trim().is_empty() {
                    self.error = Some("Initial rate cannot be empty — e.g. 1MiB".into());
                } else {
                    self.linear_initial = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::LinearRate => {
                if self.input.trim().is_empty() {
                    self.error = Some("Increment cannot be empty — e.g. 100KiB".into());
                } else {
                    self.linear_rate = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::MaxPrebuildCache => {
                if self.input.trim().is_empty() {
                    self.error = Some("Cannot be empty — e.g. 1GiB".into());
                } else {
                    self.max_prebuild_cache = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::MaxBlockSize => {
                if self.input.trim().is_empty() {
                    self.error = Some("Cannot be empty — e.g. 2MiB".into());
                } else {
                    self.max_block_size = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::StaticPath => {
                if self.input.trim().is_empty() {
                    self.error = Some("Path cannot be empty".into());
                } else {
                    self.static_path = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::TemplatedJsonPath => {
                if self.input.trim().is_empty() {
                    self.error = Some("Template path cannot be empty".into());
                } else {
                    self.template_path = self.input.trim().into();
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            FormRow::SavePath => {
                let path = self.input.trim().to_string();
                if path.is_empty() {
                    self.error = Some("File path cannot be empty".into());
                    return;
                }
                let yaml = self.current_yaml();
                match std::fs::write(&path, &yaml) {
                    Ok(()) => {
                        self.save_path = path.clone();
                        self.preview_config_input = path;
                        self.saved = true;
                        self.dirty = false;
                        self.save_notification = Some(Instant::now());
                        self.form_editing = false;
                    }
                    Err(e) => {
                        self.error = Some(format!("Write failed: {e}"));
                    }
                }
            }
            FormRow::BlackholeEntry(i) => {
                let addr = self.input.trim().to_string();
                if addr.is_empty() {
                    self.error = Some("Address cannot be empty (e.g. 127.0.0.1:9091)".into());
                } else {
                    if i < self.blackhole_entries.len() {
                        self.blackhole_entries[i].addr = addr;
                    }
                    self.dirty = true;
                    self.form_editing = false;
                }
            }
            _ => {
                self.form_editing = false;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Save (Ctrl+S fast path)
    // -----------------------------------------------------------------------

    fn do_save_to_path(&mut self) {
        if self.save_path.is_empty() {
            self.error =
                Some("No save path set — navigate to Save Path row and press Enter".into());
            return;
        }
        let yaml = self.current_yaml();
        match std::fs::write(&self.save_path, &yaml) {
            Ok(()) => {
                self.preview_config_input = self.save_path.clone();
                self.saved = true;
                self.dirty = false;
                self.save_notification = Some(Instant::now());
            }
            Err(e) => {
                self.error = Some(format!("Write failed: {e}"));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Auto-import
    // -----------------------------------------------------------------------

    pub fn try_auto_import(&mut self) {
        if self.auto_import_done || self.dirty {
            return;
        }
        let path = self.save_path.clone();
        if path.is_empty() {
            return;
        }
        let Ok(yaml) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(fields) = parse_config(&yaml) else {
            return;
        };
        self.apply_import(fields);
        self.auto_import_done = true;
        self.preview_config_input = path;
    }
}

/// Map an arbitrary mount path into /tmp so no root or pre-created dirs are needed.
/// `/smp-shared` → `/tmp/smp-shared`,  `/tmp/logrotate` → `/tmp/logrotate` (unchanged).
fn remap_to_tmp(mount: &str) -> String {
    let p = std::path::Path::new(mount);
    if p.starts_with("/tmp") {
        return mount.to_string();
    }
    let name = p
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("lading_mount");
    format!("/tmp/{name}")
}

/// Return a copy of the YAML with the logrotate_fs mount_point replaced.
fn patch_mount_point(yaml: &str, new_mount: &str) -> String {
    let Ok(mut root) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return yaml.to_string();
    };
    if let Some(lfs) = root
        .get_mut("generator")
        .and_then(|v| v.get_mut(0))
        .and_then(|v| v.get_mut("file_gen"))
        .and_then(|v| v.get_mut("logrotate_fs"))
    {
        lfs["mount_point"] = serde_yaml::Value::String(new_mount.to_string());
    }
    serde_yaml::to_string(&root).unwrap_or_else(|_| yaml.to_string())
}

/// Return true if the config has a non-empty `blackhole:` section.
fn config_has_blackhole(yaml: &str) -> bool {
    let Ok(root) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return false;
    };
    matches!(
        root.get("blackhole"),
        Some(serde_yaml::Value::Sequence(s)) if !s.is_empty()
    )
}

/// Return true if path looks like a rotated log: name ends in `.log.N` where N is all digits.
fn is_rotated_log(path: &std::path::Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    // Find the last ".log." in the name
    if let Some(pos) = name.rfind(".log.") {
        let suffix = &name[pos + 5..]; // skip ".log."
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Return true if `rotated` is a rotated version of `active`.
/// e.g. active=`foo.log`, rotated=`foo.log.1` → true.
fn is_rotated_of(active: &std::path::Path, rotated: &std::path::Path) -> bool {
    let Some(active_name) = active.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let Some(rot_name) = rotated.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    // rotated name must start with "{active_name}." and the suffix must be all digits.
    let prefix = format!("{active_name}.");
    if let Some(suffix) = rot_name.strip_prefix(prefix.as_str()) {
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
    } else {
        false
    }
}

/// Return a default address for a new blackhole entry based on the last entry's port.
fn next_blackhole_addr(entries: &[BlackholeEntry]) -> String {
    let port = entries
        .last()
        .and_then(|e| e.addr.rsplit(':').next())
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9090);
    format!("127.0.0.1:{}", port + 1)
}

/// Extract the mount_point from a lading YAML config string.
fn extract_mount_point(yaml: &str) -> Option<String> {
    let root: serde_yaml::Value = serde_yaml::from_str(yaml).ok()?;
    root.get("generator")?
        .get(0)?
        .get("file_gen")?
        .get("logrotate_fs")?
        .get("mount_point")?
        .as_str()
        .map(|s| s.to_string())
}
