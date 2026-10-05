//! Power profiles: advice on how hard to work the radio (layer: Use Cases,
//! policy; see `docs/architecture.md`, ADR-0007 D8).
//!
//! The idea follows bitchat's `PowerManager` (ideas only; bitchat is GPL):
//! the device's situation resolves to one of a few modes, and each mode is a
//! schedule. The library never switches anything itself. The application
//! reads the profile and applies it to its own scanning and connections,
//! and may ignore or override it.
//!
//! ```
//! use ble_gatt::power::{PowerAdvisor, PowerInputs, PowerMode};
//!
//! let advisor = PowerAdvisor::new(PowerInputs::default());
//! let mut profile = advisor.profile();          // watch::Receiver<PowerProfile>
//! advisor.update(|inputs| inputs.battery_percent = Some(8));
//! assert_eq!(profile.borrow_and_update().mode, PowerMode::UltraLowPower);
//! ```

use std::time::Duration;

use tokio::sync::watch;

use crate::entities::models::ConnectionPriority;

/// The device's situation, as the application knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PowerInputs {
    /// The app is visible to the person.
    pub foreground: bool,
    /// Battery level, 0 to 100; `None` where unknown or there is no battery
    /// (a desktop).
    pub battery_percent: Option<u8>,
    pub charging: bool,
    /// Known peers seen recently, as the application counts them.
    pub peers_nearby: usize,
}

impl Default for PowerInputs {
    /// In the foreground, on mains power, nobody nearby yet.
    fn default() -> Self {
        Self {
            foreground: true,
            battery_percent: None,
            charging: false,
            peers_nearby: 0,
        }
    }
}

/// How hard to work the radio, from most to least.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PowerMode {
    Performance,
    Balanced,
    PowerSaver,
    UltraLowPower,
}

/// One mode's schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PowerProfile {
    pub mode: PowerMode,
    /// Scan for this long ...
    pub scan_on: Duration,
    /// ... then pause for this long. Zero means scan continuously.
    pub scan_off: Duration,
    /// Keep at most this many connections open.
    pub max_connections: usize,
    /// Ask for this on each connection (`Connection::request_connection_priority`).
    pub connection_priority: ConnectionPriority,
}

/// The rules that pick a mode, and each mode's schedule. `Default` is the
/// library's advice; an application can change any field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PowerPolicy {
    /// At or below this battery level, and not charging: `UltraLowPower`.
    pub critical_battery_percent: u8,
    /// At or below this battery level, and not charging: `PowerSaver`.
    pub low_battery_percent: u8,
    pub performance: PowerProfile,
    pub balanced: PowerProfile,
    pub power_saver: PowerProfile,
    pub ultra_low_power: PowerProfile,
}

impl Default for PowerPolicy {
    fn default() -> Self {
        Self {
            critical_battery_percent: 10,
            low_battery_percent: 20,
            performance: PowerProfile {
                mode: PowerMode::Performance,
                scan_on: Duration::from_secs(10),
                scan_off: Duration::ZERO,
                max_connections: 8,
                connection_priority: ConnectionPriority::High,
            },
            balanced: PowerProfile {
                mode: PowerMode::Balanced,
                scan_on: Duration::from_secs(5),
                scan_off: Duration::from_secs(10),
                max_connections: 6,
                connection_priority: ConnectionPriority::Balanced,
            },
            power_saver: PowerProfile {
                mode: PowerMode::PowerSaver,
                scan_on: Duration::from_secs(3),
                scan_off: Duration::from_secs(30),
                max_connections: 3,
                connection_priority: ConnectionPriority::LowPower,
            },
            ultra_low_power: PowerProfile {
                mode: PowerMode::UltraLowPower,
                scan_on: Duration::from_secs(2),
                scan_off: Duration::from_secs(60),
                max_connections: 1,
                connection_priority: ConnectionPriority::LowPower,
            },
        }
    }
}

impl PowerPolicy {
    /// The mode for `inputs`, first rule that matches:
    ///
    /// 1. charging: `Performance`;
    /// 2. battery at or below `critical_battery_percent`: `UltraLowPower`;
    /// 3. battery at or below `low_battery_percent`: `PowerSaver`;
    /// 4. foreground: `Balanced`;
    /// 5. background with peers nearby: `Balanced`, to keep their links;
    /// 6. background with nobody nearby: `PowerSaver`.
    pub fn mode(&self, inputs: &PowerInputs) -> PowerMode {
        if inputs.charging {
            return PowerMode::Performance;
        }
        match inputs.battery_percent {
            Some(level) if level <= self.critical_battery_percent => return PowerMode::UltraLowPower,
            Some(level) if level <= self.low_battery_percent => return PowerMode::PowerSaver,
            _ => {}
        }
        if inputs.foreground || inputs.peers_nearby > 0 {
            PowerMode::Balanced
        } else {
            PowerMode::PowerSaver
        }
    }

    pub fn profile(&self, mode: PowerMode) -> PowerProfile {
        match mode {
            PowerMode::Performance => self.performance,
            PowerMode::Balanced => self.balanced,
            PowerMode::PowerSaver => self.power_saver,
            PowerMode::UltraLowPower => self.ultra_low_power,
        }
    }

    pub fn resolve(&self, inputs: &PowerInputs) -> PowerProfile {
        self.profile(self.mode(inputs))
    }
}

/// Publishes the profile for the current inputs (pub/sub, as the rest of
/// the library): the application updates the inputs as the device's
/// situation changes, and every subscriber sees the profile change.
pub struct PowerAdvisor {
    policy: PowerPolicy,
    inputs: watch::Sender<PowerInputs>,
    profile: watch::Sender<PowerProfile>,
}

impl PowerAdvisor {
    pub fn new(inputs: PowerInputs) -> Self {
        Self::with_policy(PowerPolicy::default(), inputs)
    }

    pub fn with_policy(policy: PowerPolicy, inputs: PowerInputs) -> Self {
        let profile = policy.resolve(&inputs);
        Self {
            policy,
            inputs: watch::Sender::new(inputs),
            profile: watch::Sender::new(profile),
        }
    }

    /// Changes the inputs and republishes the profile if it changed.
    pub fn update(&self, change: impl FnOnce(&mut PowerInputs)) {
        self.inputs.send_modify(change);
        let profile = self.policy.resolve(&self.inputs.borrow());
        self.profile.send_if_modified(|current| {
            let changed = *current != profile;
            *current = profile;
            changed
        });
    }

    pub fn inputs(&self) -> PowerInputs {
        self.inputs.borrow().clone()
    }

    /// The advised profile, published: each receiver sees the current value
    /// at once and every change after it.
    pub fn profile(&self) -> watch::Receiver<PowerProfile> {
        self.profile.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(foreground: bool, battery: Option<u8>, charging: bool, peers: usize) -> PowerInputs {
        PowerInputs {
            foreground,
            battery_percent: battery,
            charging,
            peers_nearby: peers,
        }
    }

    #[test]
    fn rules_apply_in_order() {
        let policy = PowerPolicy::default();
        assert_eq!(policy.mode(&inputs(false, Some(5), true, 0)), PowerMode::Performance);
        assert_eq!(policy.mode(&inputs(true, Some(10), false, 3)), PowerMode::UltraLowPower);
        assert_eq!(policy.mode(&inputs(true, Some(20), false, 3)), PowerMode::PowerSaver);
        assert_eq!(policy.mode(&inputs(true, Some(21), false, 0)), PowerMode::Balanced);
        assert_eq!(policy.mode(&inputs(false, None, false, 2)), PowerMode::Balanced);
        assert_eq!(policy.mode(&inputs(false, None, false, 0)), PowerMode::PowerSaver);
    }

    #[test]
    fn an_application_can_change_the_policy() {
        let policy = PowerPolicy {
            low_battery_percent: 50,
            ..PowerPolicy::default()
        };
        assert_eq!(policy.mode(&inputs(true, Some(40), false, 0)), PowerMode::PowerSaver);
    }

    #[test]
    fn the_advisor_publishes_only_real_changes() {
        let advisor = PowerAdvisor::new(PowerInputs::default());
        let mut profile = advisor.profile();
        assert_eq!(profile.borrow_and_update().mode, PowerMode::Balanced);

        // Same mode: nothing published.
        advisor.update(|inputs| inputs.peers_nearby = 4);
        assert!(!profile.has_changed().unwrap());

        advisor.update(|inputs| inputs.foreground = false);
        advisor.update(|inputs| inputs.peers_nearby = 0);
        assert!(profile.has_changed().unwrap());
        assert_eq!(profile.borrow_and_update().mode, PowerMode::PowerSaver);
    }
}
