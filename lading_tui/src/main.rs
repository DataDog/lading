mod app;
mod config;
mod ui;
mod variants;

use std::{io, time::Duration};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

use app::App;

const HELP: &str = "\
lading_tui — interactive TUI for building and running lading configs

USAGE:
    lading_tui [OPTIONS]

OPTIONS:
    -c, --config <PATH>   Load a lading YAML config on startup and prefill
                          all form fields from it. The path is also used as
                          the default save destination.

    -v, --verbose         Show lading's stderr output in the Preview tab
                          while a run is active.

    -h, --help            Print this help message and exit.

EXAMPLES:
    lading_tui
    lading_tui --config examples/lading-custom-30-30-40.yaml
    lading_tui -c /tmp/my_config.yaml -v

KEYS (Build tab):
    ↑ ↓           Navigate form rows
    Enter         Edit field / expand section / confirm
    Esc           Cancel edit
    ← →           Cycle option (blackhole type, etc.)
    d             Delete entry (blackhole rows)
    Ctrl+S        Save config to the current Config Path
    r             Re-generate random seed
    i             Import a config from a file path
    Tab           Switch to Preview tab
    q             Quit

KEYS (Preview tab):
    r / Enter     Build lading and start a run (10 min max)
    q             Stop run / quit
    ↑ ↓           Scroll log content
    ← →           Switch between log files
    [ ]           Step back/forward through snapshots (after stop)
    Tab           Switch to Build tab
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // Handle --help / -h before touching the terminal
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{HELP}");
        return Ok(());
    }

    let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");

    // --config <path> or -c <path>
    let config_path = args.windows(2).find_map(|w| {
        if w[0] == "--config" || w[0] == "-c" {
            Some(w[1].clone())
        } else {
            None
        }
    });

    // --- set up terminal ---
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(verbose, config_path);
    let result = run(&mut terminal, &mut app);

    // --- restore terminal ---
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = result {
        eprintln!("Error: {err:?}");
    }

    if app.saved {
        println!("Config saved to: {}", app.save_path);
    }

    Ok(())
}

fn run<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    app: &mut App,
) -> io::Result<()> {
    loop {
        terminal.draw(|f| ui::draw(f, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                // Filter out key-release / repeat events (important on some platforms)
                if key.kind == KeyEventKind::Press {
                    app.handle_key(key.code, key.modifiers);
                }
            }
        }

        app.tick();

        if app.quit {
            return Ok(());
        }
    }
}
