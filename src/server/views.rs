//! Optional explicit views for sparse server collection.

use core::ops::Range;

use bevy::{
    ecs::{
        entity::{EntityHashMap, EntityHashSet},
        entity_disabling::Disabled,
        query::{IterQueryData, QueryData, QueryFilter, QueryItem},
    },
    prelude::*,
};

use super::{
    ClientVisibility,
    replication_messages::{serialized_data::SerializedData, updates::Updates},
    visibility::registry::FilterRegistry,
};
use crate::{prelude::*, shared::replication::client_ticks::ClientTicks};

/// Chooses the entities considered for each client's replication.
#[derive(Resource, Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationDomain {
    /// Consider every replicated entity, preserving the ordinary collector.
    #[default]
    Broadcast,
    /// Consider only entities in each client's [`ClientView`].
    ///
    /// A missing or empty view permits no entity delivery. Removing an entity
    /// from its view, or removing a live entity's [`Replicated`] eligibility,
    /// retires any established replica through the reliable update channel.
    Explicit,
}

/// The authoritative entities a client should receive in explicit-view mode.
///
/// Update this before [`ServerSystems::Send`]. Membership does not bypass
/// replication eligibility or existing entity and component visibility filters.
#[derive(Component, Default, Debug, Clone)]
pub struct ClientView(pub EntityHashSet);

#[derive(Resource, Default)]
pub(super) struct SendPlan {
    pub(super) explicit: bool,
    /// This send's connections, sorted; recipients are slots in this list.
    clients: Vec<Entity>,
    /// Each desired entity's entry, or [`INELIGIBLE`], resolved once per send.
    index: EntityHashMap<u32>,
    entries: Vec<PlanEntry>,
    recipients: Vec<u32>,
    /// Entry and client slot for every eligible desired pair.
    pairs: Vec<(u32, u32)>,
}

/// A desired entity that is dead or lacks [`Replicated`] this send.
const INELIGIBLE: u32 = u32::MAX;

pub(super) struct PlanEntry {
    pub(super) entity: Entity,
    recipients: Range<usize>,
}

impl SendPlan {
    pub(super) fn entries(&self) -> &[PlanEntry] {
        &self.entries
    }

    pub(super) fn recipients(&self, entry: &PlanEntry) -> &[u32] {
        &self.recipients[entry.recipients.clone()]
    }

    pub(super) fn for_entity(&self, entity: Entity) -> &[u32] {
        match self.index.get(&entity) {
            Some(&entry) if entry != INELIGIBLE => self.recipients(&self.entries[entry as usize]),
            _ => &[],
        }
    }

    pub(super) fn client(&self, slot: u32) -> Entity {
        self.clients[slot as usize]
    }
}

/// Reconciles existing replicas before mappings or native despawn cleanup can
/// consume their baselines, then freezes this send's sparse iteration domain.
pub(super) fn prepare_views(
    domain: Res<ReplicationDomain>,
    mut plan: ResMut<SendPlan>,
    registry: Res<FilterRegistry>,
    mut serialized: ResMut<SerializedData>,
    entities: Query<Has<Replicated>, Allow<Disabled>>,
    mut clients: Query<
        (
            Entity,
            Option<&ClientView>,
            &mut ClientTicks,
            &mut Updates,
            &ClientVisibility,
        ),
        With<ConnectedClient>,
    >,
    mut departures: Local<Vec<(Entity, bool)>>,
) -> Result<()> {
    let SendPlan {
        explicit,
        clients: slots,
        index,
        entries,
        recipients,
        pairs,
    } = &mut *plan;
    *explicit = *domain == ReplicationDomain::Explicit;
    slots.clear();
    index.clear();
    entries.clear();
    recipients.clear();
    pairs.clear();
    if !*explicit {
        return Ok(());
    }

    slots.extend(clients.iter().map(|(client, ..)| client));
    slots.sort_unstable();
    for (client, view, ..) in &clients {
        let Some(view) = view else {
            continue;
        };
        let slot = slots.binary_search(&client).unwrap() as u32;
        for &entity in &view.0 {
            let entry = *index.entry(entity).or_insert_with(|| {
                if matches!(entities.get(entity), Ok(true)) {
                    entries.push(PlanEntry {
                        entity,
                        recipients: 0..0,
                    });
                    entries.len() as u32 - 1
                } else {
                    INELIGIBLE
                }
            });
            if entry != INELIGIBLE {
                // Count now; recipients are grouped below without sorting pairs.
                entries[entry as usize].recipients.end += 1;
                pairs.push((entry, slot));
            }
        }
    }
    let mut start = 0;
    for entry in entries.iter_mut() {
        let count = entry.recipients.end;
        entry.recipients = start..start;
        start += count;
    }
    recipients.resize(start, 0);
    for &(entry, slot) in pairs.iter() {
        let range = &mut entries[entry as usize].recipients;
        recipients[range.end] = slot;
        range.end += 1;
    }

    for (_, view, mut ticks, mut updates, visibility) in &mut clients {
        departures.clear();
        for &entity in ticks.entities.keys() {
            if !view.is_some_and(|view| view.0.contains(&entity)) {
                // View departure must deliver teardown even when an ordinary
                // filter hides the entity or it was deleted in this frame.
                departures.push((entity, true));
                continue;
            }
            if index.get(&entity) != Some(&INELIGIBLE) {
                // A live filter hide keeps its native lost-visibility teardown.
                continue;
            }

            if entities.contains(entity) {
                // A live entity can stop participating without losing the
                // cached policy needed if it becomes eligible again.
                departures.push((entity, true));
            } else {
                // Retained hidden lifetimes deliberately keep stale client
                // state after authoritative death. WhileVisible still needs
                // teardown, including a hide and deletion in the same frame.
                let lifetime = visibility.get(entity).hidden_entity_lifetime(&registry);
                let send = lifetime.is_none_or(|l| l == ScopeLifetime::WhileVisible);
                departures.push((entity, send));
            }
        }
        for (entity, send) in departures.drain(..) {
            if send {
                let range = serialized.write_entity(entity)?;
                updates.add_despawn(range);
            }
            ticks.entities.remove(&entity);
        }
    }

    Ok(())
}

/// Preserves native query iteration in broadcast mode and visits only the
/// supplied recipient slots in explicit mode, skipping clients that no longer
/// match. Each visit looks its client up; use [`ClientSlots`] on hot paths.
pub(super) fn for_clients<D: IterQueryData, F: QueryFilter>(
    clients: &mut Query<D, F>,
    plan: &SendPlan,
    recipients: Option<&[u32]>,
    mut f: impl FnMut(QueryItem<'_, '_, D>) -> Result<()>,
) -> Result<()> {
    if let Some(recipients) = recipients {
        let mut clients = clients.iter_many_mut(recipients.iter().map(|&slot| plan.client(slot)));
        while let Some(client) = clients.fetch_next() {
            f(client)?;
        }
    } else {
        for client in clients.iter_mut() {
            f(client)?;
        }
    }
    Ok(())
}

/// Connections fetched once per collection system. Explicit sends then address
/// each recipient by plan slot instead of fetching it for every entity and
/// component.
pub(super) struct ClientSlots<'q, 's, D: QueryData> {
    explicit: bool,
    items: Vec<Option<QueryItem<'q, 's, D>>>,
}

impl<'q, 's, D: IterQueryData> ClientSlots<'q, 's, D> {
    pub(super) fn new<F: QueryFilter>(
        clients: &'q mut Query<'_, 's, D, F>,
        plan: &SendPlan,
        entity: impl Fn(&QueryItem<'q, 's, D>) -> Entity,
    ) -> Self {
        let mut items = Vec::new();
        if plan.explicit {
            items.resize_with(plan.clients.len(), || None);
            for item in clients.iter_mut() {
                if let Ok(slot) = plan.clients.binary_search(&entity(&item)) {
                    items[slot] = Some(item);
                }
            }
        } else {
            items.extend(clients.iter_mut().map(Some));
        }
        Self {
            explicit: plan.explicit,
            items,
        }
    }

    /// Visits every connection in broadcast mode, otherwise only `recipients`.
    pub(super) fn for_each(
        &mut self,
        recipients: &[u32],
        mut f: impl FnMut(&mut QueryItem<'q, 's, D>) -> Result<()>,
    ) -> Result<()> {
        if self.explicit {
            for &slot in recipients {
                if let Some(item) = &mut self.items[slot as usize] {
                    f(item)?;
                }
            }
        } else {
            for item in self.items.iter_mut().flatten() {
                f(item)?;
            }
        }
        Ok(())
    }
}
