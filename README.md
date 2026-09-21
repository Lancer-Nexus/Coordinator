# Lancer Nexus Coordinator

The Coordinator maintains cluster membership and makes placement decisions for Lancer Nexus game instances.

## Responsibilities

- Register Agents and game instances
- Track readiness, capacity, draining and failure state
- Assign players and groups to suitable instances
- Preserve group and formation affinity
- Reserve event-server capacity
- Coordinate transfers without owning the game simulation
- Expose operator and health information

The Coordinator is a control-plane service. It does not become the authoritative source for player inventory, credits or world state.
