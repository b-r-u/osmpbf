//! Nodes, ways and relations

use crate::block::{get_stringtable_key_value, str_from_stringtable};
use crate::dense::DenseNode;
use crate::error::Result;
use crate::proto::osmformat;
use crate::proto::osmformat::PrimitiveBlock;
use osmformat::relation::MemberType;
use protobuf::EnumOrUnknown;

/// An enum with the OSM core elements: nodes, ways and relations.
#[derive(Clone, Debug)]
pub enum Element<'a> {
    /// A node. Also, see [`DenseNode`](Self::DenseNode).
    Node(Node<'a>),

    /// Just like [`Node`](Self::Node), but with a different representation in memory. This distinction is
    /// usually not important but is not abstracted away to avoid copying. So, if you want to match
    /// `Node`, you also likely want to match [`DenseNode`].
    DenseNode(DenseNode<'a>),

    /// A way.
    Way(Way<'a>),

    /// A relation.
    Relation(Relation<'a>),
}

/// An OpenStreetMap node element (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Node)).
#[derive(Clone, Debug)]
pub struct Node<'a> {
    block: &'a PrimitiveBlock,
    osmnode: &'a osmformat::Node,
}

impl<'a> Node<'a> {
    pub(crate) fn new(block: &'a PrimitiveBlock, osmnode: &'a osmformat::Node) -> Node<'a> {
        Node { block, osmnode }
    }

    /// Returns the node id. It should be unique between nodes and might be negative to indicate
    /// that the element has not yet been uploaded to a server.
    pub fn id(&self) -> i64 {
        self.osmnode.id()
    }

    /// Returns an iterator over the tags of this node
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of strings (key and value).
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    ///
    /// reader.for_each(|element| {
    ///     if let Element::Node(node) = element {
    ///         for (key, value) in node.tags() {
    ///             println!("key: {key}, value: {value}");
    ///         }
    ///     }
    /// })?;
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn tags(&self) -> TagIter<'a> {
        TagIter {
            block: self.block,
            key_indices: self.osmnode.keys.iter(),
            val_indices: self.osmnode.vals.iter(),
        }
    }

    /// Returns additional metadata for this element.
    pub fn info(&self) -> Info<'a> {
        Info::new(self.block, self.osmnode.info.get_or_default())
    }

    /// Returns the latitude coordinate in degrees.
    pub fn lat(&self) -> f64 {
        1e-9 * self.nano_lat() as f64
    }

    /// Returns the latitude coordinate in nanodegrees (10⁻⁹).
    pub fn nano_lat(&self) -> i64 {
        self.block.lat_offset() + i64::from(self.block.granularity()) * self.osmnode.lat()
    }

    /// Returns the latitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lat(&self) -> i32 {
        (self.nano_lat() / 100) as i32
    }

    /// Returns the longitude coordinate in degrees.
    pub fn lon(&self) -> f64 {
        1e-9 * self.nano_lon() as f64
    }

    /// Returns the longitude in nanodegrees (10⁻⁹).
    pub fn nano_lon(&self) -> i64 {
        self.block.lon_offset() + i64::from(self.block.granularity()) * self.osmnode.lon()
    }

    /// Returns the longitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lon(&self) -> i32 {
        (self.nano_lon() / 100) as i32
    }

    /// Returns an iterator over the tags of this node
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of indices (key and value) to the stringtable of the current
    /// [`PrimitiveBlock`](crate::block::PrimitiveBlock).
    pub fn raw_tags(&self) -> RawTagIter<'a> {
        RawTagIter {
            key_indices: self.osmnode.keys.iter(),
            val_indices: self.osmnode.vals.iter(),
        }
    }

    /// Returns the raw stringtable. Elements in a `PrimitiveBlock` do not store strings
    /// themselves; instead, they just store indices to a common stringtable. By convention, the
    /// contained strings are UTF-8 encoded but it is not safe to assume that (use
    /// `std::str::from_utf8`).
    pub fn raw_stringtable(&self) -> &[Vec<u8>] {
        self.block.stringtable.s.as_slice()
    }
}

/// An OpenStreetMap way element (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Way)).
///
/// A way contains an ordered list of node references that can be accessed with the `refs` or the
/// `raw_refs` method.
#[derive(Clone, Debug)]
pub struct Way<'a> {
    block: &'a PrimitiveBlock,
    osmway: &'a osmformat::Way,
}

impl<'a> Way<'a> {
    pub(crate) fn new(block: &'a PrimitiveBlock, osmway: &'a osmformat::Way) -> Way<'a> {
        Way { block, osmway }
    }

    /// Returns the way id.
    pub fn id(&self) -> i64 {
        self.osmway.id()
    }

    /// Returns an iterator over the tags of this way
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of strings (key and value).
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    ///
    /// reader.for_each(|element| {
    ///     if let Element::Way(way) = element {
    ///         for (key, value) in way.tags() {
    ///             println!("key: {key}, value: {value}");
    ///         }
    ///     }
    /// })?;
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn tags(&self) -> TagIter<'a> {
        TagIter {
            block: self.block,
            key_indices: self.osmway.keys.iter(),
            val_indices: self.osmway.vals.iter(),
        }
    }

    /// Returns additional metadata for this element.
    pub fn info(&self) -> Info<'a> {
        Info::new(self.block, self.osmway.info.get_or_default())
    }

    /// Returns an iterator over the references of this way. Each reference should correspond to a
    /// node id.
    ///
    /// Finding the corresponding node might involve iterating over the whole PBF structure, but
    /// (to save space) ways themselves usually do not contain geo coordinates.
    pub fn refs(&self) -> WayRefIter<'a> {
        WayRefIter {
            deltas: self.osmway.refs.iter(),
            current: 0,
        }
    }

    /// Returns an iterator over the way's node locations (latitude, longitude).
    /// Only available if the optional `LocationsOnWays` feature is included in the
    /// [`HeaderBlock`](crate::block::HeaderBlock) and should return an empty iterator otherwise
    /// (See the [`optional_features`](crate::block::HeaderBlock::optional_features) method).
    ///
    /// Use [`refs`](Way::refs) if this feature is not present or to get information other than
    /// coordinates about the nodes that constitute a way.
    pub fn node_locations(&self) -> WayNodeLocationsIter<'a> {
        WayNodeLocationsIter {
            block: self.block,
            dlats: self.osmway.lat.iter(),
            dlons: self.osmway.lon.iter(),
            clat: 0,
            clon: 0,
        }
    }

    /// Returns a slice of delta coded node ids.
    pub fn raw_refs(&self) -> &[i64] {
        self.osmway.refs.as_slice()
    }

    /// Returns an iterator over the tags of this way
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of indices (key and value) to the stringtable of the current
    /// [`PrimitiveBlock`](crate::block::PrimitiveBlock).
    pub fn raw_tags(&self) -> RawTagIter<'a> {
        RawTagIter {
            key_indices: self.osmway.keys.iter(),
            val_indices: self.osmway.vals.iter(),
        }
    }

    /// Returns the raw stringtable. Elements in a `PrimitiveBlock` do not store strings
    /// themselves; instead, they just store indices to a common stringtable. By convention, the
    /// contained strings are UTF-8 encoded but it is not safe to assume that (use
    /// `std::str::from_utf8`).
    pub fn raw_stringtable(&self) -> &[Vec<u8>] {
        self.block.stringtable.s.as_slice()
    }
}

/// An OpenStreetMap relation element (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Relation)).
///
/// A relation contains an ordered list of members that can be of any element type.
#[derive(Clone, Debug)]
pub struct Relation<'a> {
    block: &'a PrimitiveBlock,
    osmrel: &'a osmformat::Relation,
}

impl<'a> Relation<'a> {
    pub(crate) fn new(block: &'a PrimitiveBlock, osmrel: &'a osmformat::Relation) -> Relation<'a> {
        Relation { block, osmrel }
    }

    /// Returns the relation id.
    pub fn id(&self) -> i64 {
        self.osmrel.id()
    }

    /// Returns an iterator over the tags of this relation
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of strings (key and value).
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    ///
    /// reader.for_each(|element| {
    ///     if let Element::Relation(relation) = element {
    ///         for (key, value) in relation.tags() {
    ///             println!("key: {key}, value: {value}");
    ///         }
    ///     }
    /// })?;
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn tags(&self) -> TagIter<'a> {
        TagIter {
            block: self.block,
            key_indices: self.osmrel.keys.iter(),
            val_indices: self.osmrel.vals.iter(),
        }
    }

    /// Returns additional metadata for this element.
    pub fn info(&self) -> Info<'a> {
        Info::new(self.block, self.osmrel.info.get_or_default())
    }

    /// Returns an iterator over the members of this relation.
    pub fn members(&self) -> RelMemberIter<'a> {
        RelMemberIter::new(self.block, self.osmrel)
    }

    /// Returns an iterator over the tags of this relation
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of indices (key and value) to the stringtable of the current
    /// [`PrimitiveBlock`](crate::block::PrimitiveBlock).
    pub fn raw_tags(&self) -> RawTagIter<'a> {
        RawTagIter {
            key_indices: self.osmrel.keys.iter(),
            val_indices: self.osmrel.vals.iter(),
        }
    }

    /// Returns the raw stringtable. Elements in a `PrimitiveBlock` do not store strings
    /// themselves; instead, they just store indices to a common stringtable. By convention, the
    /// contained strings are UTF-8 encoded but it is not safe to assume that (use
    /// `std::str::from_utf8`).
    pub fn raw_stringtable(&self) -> &[Vec<u8>] {
        self.block.stringtable.s.as_slice()
    }
}

/// An iterator over the references of a way.
///
/// Each reference corresponds to a node id.
#[derive(Clone, Debug)]
pub struct WayRefIter<'a> {
    deltas: std::slice::Iter<'a, i64>,
    current: i64,
}

impl<'a> Iterator for WayRefIter<'a> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match self.deltas.next() {
            Some(&d) => {
                self.current += d;
                Some(self.current)
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.deltas.size_hint()
    }
}

impl<'a> ExactSizeIterator for WayRefIter<'a> {}

pub struct WayNodeLocation {
    lat: i64,
    lon: i64,
}

/// A node location that contains latitude and longitude coordinates.
impl WayNodeLocation {
    /// Returns the latitude coordinate in degrees.
    pub fn lat(&self) -> f64 {
        1e-9 * self.nano_lat() as f64
    }

    /// Returns the latitude coordinate in nanodegrees (10⁻⁹).
    pub fn nano_lat(&self) -> i64 {
        self.lat
    }

    /// Returns the latitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lat(&self) -> i32 {
        (self.nano_lat() / 100) as i32
    }

    /// Returns the longitude coordinate in degrees.
    pub fn lon(&self) -> f64 {
        1e-9 * self.nano_lon() as f64
    }

    /// Returns the longitude in nanodegrees (10⁻⁹).
    pub fn nano_lon(&self) -> i64 {
        self.lon
    }

    /// Returns the longitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lon(&self) -> i32 {
        (self.nano_lon() / 100) as i32
    }
}

/// An iterator over the node locations of a way.
/// Each element is a pair of coordinates consisting of latitude and longitude.
#[derive(Clone, Debug)]
pub struct WayNodeLocationsIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    dlats: std::slice::Iter<'a, i64>,
    dlons: std::slice::Iter<'a, i64>,
    clat: i64,
    clon: i64,
}

impl<'a> Iterator for WayNodeLocationsIter<'a> {
    type Item = WayNodeLocation;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.dlats.next(), self.dlons.next()) {
            (Some(&dlat), Some(&dlon)) => {
                self.clat += dlat;
                self.clon += dlon;
                Some(WayNodeLocation {
                    lat: self.block.lat_offset() + i64::from(self.block.granularity()) * self.clat,
                    lon: self.block.lon_offset() + i64::from(self.block.granularity()) * self.clon,
                })
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.dlats.size_hint()
    }
}

impl<'a> ExactSizeIterator for WayNodeLocationsIter<'a> {}

/// The element type of a relation member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelMemberType {
    Node,
    Way,
    Relation,
}

impl From<EnumOrUnknown<MemberType>> for RelMemberType {
    fn from(rmt: EnumOrUnknown<MemberType>) -> RelMemberType {
        match rmt.unwrap() {
            MemberType::NODE => RelMemberType::Node,
            MemberType::WAY => RelMemberType::Way,
            MemberType::RELATION => RelMemberType::Relation,
        }
    }
}

//TODO encapsulate member_id based on member_type (NodeId, WayId, RelationId)
/// A member of a relation.
///
/// Each member has a member type and a member id that references an element of that type.
#[derive(Clone, Debug)]
pub struct RelMember<'a> {
    block: &'a PrimitiveBlock,
    pub role_sid: i32,
    pub member_id: i64,
    pub member_type: RelMemberType,
}

impl<'a> RelMember<'a> {
    /// Returns the role of a relation member.
    pub fn role(&self) -> Result<&'a str> {
        str_from_stringtable(self.block, self.role_sid as usize)
    }
}

/// An iterator over the members of a relation.
#[derive(Clone, Debug)]
pub struct RelMemberIter<'a> {
    block: &'a PrimitiveBlock,
    role_sids: std::slice::Iter<'a, i32>,
    member_id_deltas: std::slice::Iter<'a, i64>,
    member_types: std::slice::Iter<'a, EnumOrUnknown<MemberType>>,
    current_member_id: i64,
}

impl<'a> RelMemberIter<'a> {
    fn new(block: &'a PrimitiveBlock, osmrel: &'a osmformat::Relation) -> RelMemberIter<'a> {
        RelMemberIter {
            block,
            role_sids: osmrel.roles_sid.iter(),
            member_id_deltas: osmrel.memids.iter(),
            member_types: osmrel.types.iter(),
            current_member_id: 0,
        }
    }
}

impl<'a> Iterator for RelMemberIter<'a> {
    type Item = RelMember<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.role_sids.next(),
            self.member_id_deltas.next(),
            self.member_types.next(),
        ) {
            (Some(role_sid), Some(mem_id_delta), Some(member_type)) => {
                self.current_member_id += *mem_id_delta;
                Some(RelMember {
                    block: self.block,
                    role_sid: *role_sid,
                    member_id: self.current_member_id,
                    member_type: RelMemberType::from(*member_type),
                })
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.role_sids.size_hint()
    }
}

impl<'a> ExactSizeIterator for RelMemberIter<'a> {}

/// An iterator over the tags of an element. It returns a pair of strings (key and value).
#[derive(Clone, Debug)]
pub struct TagIter<'a> {
    block: &'a PrimitiveBlock,
    key_indices: std::slice::Iter<'a, u32>,
    val_indices: std::slice::Iter<'a, u32>,
}

//TODO return Result?
impl<'a> Iterator for TagIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        get_stringtable_key_value(
            self.block,
            self.key_indices.next().map(|v| *v as usize),
            self.val_indices.next().map(|v| *v as usize),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.key_indices.size_hint()
    }
}

impl<'a> ExactSizeIterator for TagIter<'a> {}

/// An iterator over the tags of an element. It returns a pair of indices (key and value) to the
/// stringtable of the current [`PrimitiveBlock`](crate::block::PrimitiveBlock).
#[derive(Clone, Debug)]
pub struct RawTagIter<'a> {
    key_indices: std::slice::Iter<'a, u32>,
    val_indices: std::slice::Iter<'a, u32>,
}

//TODO return Result?
impl<'a> Iterator for RawTagIter<'a> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.key_indices.next(), self.val_indices.next()) {
            (Some(&key_index), Some(&val_index)) => Some((key_index, val_index)),
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.key_indices.size_hint()
    }
}

impl<'a> ExactSizeIterator for RawTagIter<'a> {}

/// Additional metadata that might be included in each element.
#[derive(Clone, Debug)]
pub struct Info<'a> {
    block: &'a PrimitiveBlock,
    info: &'a osmformat::Info,
}

impl<'a> Info<'a> {
    fn new(block: &'a PrimitiveBlock, info: &'a osmformat::Info) -> Info<'a> {
        Info { block, info }
    }

    /// Returns the version of this element.
    pub fn version(&self) -> Option<i32> {
        self.info.version
    }

    /// Returns the time stamp in milliseconds since the epoch.
    pub fn milli_timestamp(&self) -> Option<i64> {
        if self.info.has_timestamp() {
            Some(self.info.timestamp() * i64::from(self.block.date_granularity()))
        } else {
            None
        }
    }

    /// Returns the changeset id.
    pub fn changeset(&self) -> Option<i64> {
        self.info.changeset
    }

    /// Returns the user id.
    pub fn uid(&self) -> Option<i32> {
        self.info.uid
    }

    /// Returns the user name.
    pub fn user(&self) -> Option<Result<&'a str>> {
        if self.info.has_user_sid() {
            Some(str_from_stringtable(
                self.block,
                self.info.user_sid() as usize,
            ))
        } else {
            None
        }
    }

    /// Returns the visibility status of an element. This is only relevant if the PBF file contains
    /// historical information.
    pub fn visible(&self) -> bool {
        // If the visible flag is not present it must be assumed to be true.
        self.info.visible.unwrap_or(true)
    }

    /// Returns true if the element was deleted.
    /// This is a convenience function that just returns the inverse of [`Info::visible`].
    pub fn deleted(&self) -> bool {
        !self.visible()
    }
}
