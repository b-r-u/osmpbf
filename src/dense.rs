//! Iterate over the dense nodes in a `PrimitiveGroup`

use crate::block::{get_stringtable_key_value, str_from_stringtable};
use crate::error::Result;
use crate::proto::osmformat;
use delta_encoding::{DeltaDecoderExt, DeltaDecoderIter};
use std;
use std::iter::Copied;
use std::slice::Iter as SliceIter;

pub(crate) type DeltaIter<'a, T> = DeltaDecoderIter<Copied<SliceIter<'a, T>>>;

//TODO Add getter functions for id, version, uid, ...
/// An OpenStreetMap node element from a compressed array of dense nodes (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Node)).
#[derive(Clone, Debug)]
pub struct DenseNode<'a> {
    block: &'a osmformat::PrimitiveBlock,

    /// The node id. It should be unique between nodes and might be negative to indicate
    /// that the element has not yet been uploaded to a server.
    pub id: i64,
    lat: i64,
    lon: i64,
    keys_vals_indices: &'a [i32],
    info: Option<DenseNodeInfo<'a>>,
}

impl<'a> DenseNode<'a> {
    /// Returns the node id. It should be unique between nodes and might be negative to indicate
    /// that the element has not yet been uploaded to a server.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// return optional metadata about the node
    pub fn info(&'a self) -> Option<&'a DenseNodeInfo<'a>> {
        self.info.as_ref()
    }

    /// Returns the latitude coordinate in degrees.
    pub fn lat(&self) -> f64 {
        1e-9 * self.nano_lat() as f64
    }

    /// Returns the latitude coordinate in nanodegrees (10⁻⁹).
    pub fn nano_lat(&self) -> i64 {
        self.block.lat_offset() + i64::from(self.block.granularity()) * self.lat
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
        self.block.lon_offset() + i64::from(self.block.granularity()) * self.lon
    }

    /// Returns the longitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lon(&self) -> i32 {
        (self.nano_lon() / 100) as i32
    }

    /// Returns an iterator over the tags of this node (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    pub fn tags(&self) -> DenseTagIter<'a> {
        DenseTagIter {
            block: self.block,
            keys_vals_indices: self.keys_vals_indices.iter(),
        }
    }

    /// Returns an iterator over the tags of this node
    /// (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Tags)).
    /// A tag is represented as a pair of indices (key and value) to the stringtable of the current
    /// [`PrimitiveBlock`](crate::block::PrimitiveBlock).
    pub fn raw_tags(&self) -> DenseRawTagIter<'a> {
        DenseRawTagIter {
            keys_vals_indices: self.keys_vals_indices.iter(),
        }
    }
}

/// An iterator over dense nodes. It decodes the delta encoded values.
#[derive(Clone, Debug)]
pub struct DenseNodeIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    ids: DeltaIter<'a, i64>,
    lats: DeltaIter<'a, i64>,
    lons: DeltaIter<'a, i64>,
    keys_vals_slice: &'a [i32],
    keys_vals_index: usize,
    info_iter: Option<DenseNodeInfoIter<'a>>,
}

impl<'a> DenseNodeIter<'a> {
    pub(crate) fn new(
        block: &'a osmformat::PrimitiveBlock,
        osmdense: &'a osmformat::DenseNodes,
    ) -> DenseNodeIter<'a> {
        let info_iter = Some(DenseNodeInfoIter::new(
            block,
            osmdense.denseinfo.get_or_default(),
        ));
        DenseNodeIter {
            block,
            ids: osmdense.id.iter().copied().original(),
            lats: osmdense.lat.iter().copied().original(),
            lons: osmdense.lon.iter().copied().original(),
            keys_vals_slice: osmdense.keys_vals.as_slice(),
            keys_vals_index: 0,
            info_iter,
        }
    }

    pub(crate) fn empty(block: &'a osmformat::PrimitiveBlock) -> DenseNodeIter<'a> {
        DenseNodeIter {
            block,
            ids: [].iter().copied().original(),
            lats: [].iter().copied().original(),
            lons: [].iter().copied().original(),
            keys_vals_slice: &[],
            keys_vals_index: 0,
            info_iter: None,
        }
    }
}

impl<'a> Iterator for DenseNodeIter<'a> {
    type Item = DenseNode<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.ids.next(),
            self.lats.next(),
            self.lons.next(),
            self.info_iter.as_mut().and_then(|iter| iter.next()),
        ) {
            (Some(id), Some(lat), Some(lon), info) => {
                let start_index = self.keys_vals_index;
                let mut end_index = start_index;
                for chunk in self.keys_vals_slice[self.keys_vals_index..].chunks(2) {
                    if chunk[0] != 0 && chunk.len() == 2 {
                        end_index += 2;
                        self.keys_vals_index += 2;
                    } else {
                        self.keys_vals_index += 1;
                        break;
                    }
                }

                Some(DenseNode {
                    block: self.block,
                    id,
                    lat,
                    lon,
                    keys_vals_indices: &self.keys_vals_slice[start_index..end_index],
                    info,
                })
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ids.size_hint()
    }
}

impl<'a> ExactSizeIterator for DenseNodeIter<'a> {}

/// Optional metadata with non-geographic information about a dense node
#[derive(Clone, Debug)]
pub struct DenseNodeInfo<'a> {
    block: &'a osmformat::PrimitiveBlock,
    /// The version of this element.
    version: i32,
    /// Timestamp
    timestamp: i64,
    /// The changeset id.
    changeset: i64,
    /// The user id.
    uid: i32,
    /// String IDs for usernames.
    user_sid: i32,
    /// Is the element visible (true) or was it deleted (false).
    visible: bool,
}

impl<'a> DenseNodeInfo<'a> {
    /// Returns the version of this element.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns the changeset id.
    pub fn changeset(&self) -> i64 {
        self.changeset
    }

    /// Returns the user id.
    pub fn uid(&self) -> i32 {
        self.uid
    }

    /// Returns the user name.
    pub fn user(&self) -> Result<&'a str> {
        str_from_stringtable(self.block, self.user_sid as usize)
    }

    /// Returns the time stamp in milliseconds since the epoch.
    pub fn milli_timestamp(&self) -> i64 {
        self.timestamp * i64::from(self.block.date_granularity())
    }

    /// Returns the visibility status of an element. This is only relevant if the PBF file contains
    /// historical information.
    pub fn visible(&self) -> bool {
        self.visible
    }

    /// Returns true if the element was deleted.
    /// This is a convenience function that just returns the inverse of `DenseNodeInfo::visible`.
    pub fn deleted(&self) -> bool {
        !self.visible
    }
}

/// An iterator over dense nodes info. It decodes the delta encoded values.
#[derive(Clone, Debug)]
pub struct DenseNodeInfoIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    versions: SliceIter<'a, i32>,
    timestamps: DeltaIter<'a, i64>,
    changesets: DeltaIter<'a, i64>,
    uids: DeltaIter<'a, i32>,
    user_sids: DeltaIter<'a, i32>,
    visible: SliceIter<'a, bool>,
}

impl<'a> DenseNodeInfoIter<'a> {
    fn new(
        block: &'a osmformat::PrimitiveBlock,
        info: &'a osmformat::DenseInfo,
    ) -> DenseNodeInfoIter<'a> {
        DenseNodeInfoIter {
            block,
            versions: info.version.iter(),
            timestamps: info.timestamp.iter().copied().original(),
            changesets: info.changeset.iter().copied().original(),
            uids: info.uid.iter().copied().original(),
            user_sids: info.user_sid.iter().copied().original(),
            visible: info.visible.iter(),
        }
    }
}

impl<'a> Iterator for DenseNodeInfoIter<'a> {
    type Item = DenseNodeInfo<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.versions.next(),
            self.timestamps.next(),
            self.changesets.next(),
            self.uids.next(),
            self.user_sids.next(),
            self.visible.next(),
        ) {
            (
                Some(&version),
                Some(timestamp),
                Some(changeset),
                Some(uid),
                Some(user_sid),
                visible_opt,
            ) => Some(DenseNodeInfo {
                block: self.block,
                version,
                timestamp,
                changeset,
                uid,
                user_sid,
                visible: *visible_opt.unwrap_or(&true),
            }),
            _ => None,
        }
    }
}

/// An iterator over the tags in a dense node.
#[derive(Clone, Debug)]
pub struct DenseTagIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    keys_vals_indices: SliceIter<'a, i32>,
}

//TODO return Result
impl<'a> Iterator for DenseTagIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        get_stringtable_key_value(
            self.block,
            self.keys_vals_indices.next().map(|v| *v as usize),
            self.keys_vals_indices.next().map(|v| *v as usize),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.keys_vals_indices.len() / 2;
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for DenseTagIter<'a> {}

/// An iterator over the tags of a node. It returns a pair of indices (key and value) to the
/// stringtable of the current [`PrimitiveBlock`](crate::block::PrimitiveBlock).
#[derive(Clone, Debug)]
pub struct DenseRawTagIter<'a> {
    keys_vals_indices: SliceIter<'a, i32>,
}

//TODO return Result
impl<'a> Iterator for DenseRawTagIter<'a> {
    type Item = (i32, i32);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.keys_vals_indices.next(), self.keys_vals_indices.next()) {
            (Some(&key_index), Some(&val_index)) => Some((key_index, val_index)),
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.keys_vals_indices.len() / 2;
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for DenseRawTagIter<'a> {}
