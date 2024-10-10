//! Iterate over the dense nodes in a `PrimitiveGroup`

use crate::elements::{MaybeDenseRawTagIter, RawNodeData};
use crate::proto::osmformat;
use std;

//TODO Add getter functions for id, version, uid, ...
/// An OpenStreetMap node element from a compressed array of dense nodes (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Node)).
#[derive(Clone, Debug)]
pub struct DenseRawNode<'a> {
    /// The node id. It should be unique between nodes and might be negative to indicate
    /// that the element has not yet been uploaded to a server.
    pub id: i64,
    lat: i64,
    lon: i64,
    keys_vals_indices: &'a [i32],
    info: osmformat::Info,
}

impl<'a> RawNodeData<'a> for DenseRawNode<'a> {
    fn id(&self) -> i64 {
        self.id
    }
    fn lat(&self) -> i64 {
        self.lat
    }
    fn lon(&self) -> i64 {
        self.lon
    }
    fn raw_tags(&self) -> MaybeDenseRawTagIter<'a> {
        MaybeDenseRawTagIter::Dense(DenseRawTagIter {
            keys_vals_indices: self.keys_vals_indices.iter(),
        })
    }
    fn info(&'a self) -> &'a osmformat::Info {
        &self.info
    }
}

/// An iterator over dense nodes. It decodes the delta encoded values.
#[derive(Clone, Debug)]
pub struct DenseRawNodeIter<'a> {
    dids: std::slice::Iter<'a, i64>,  // deltas
    cid: i64,                         // current id
    dlats: std::slice::Iter<'a, i64>, // deltas
    clat: i64,
    dlons: std::slice::Iter<'a, i64>, // deltas
    clon: i64,
    keys_vals_slice: &'a [i32],
    keys_vals_index: usize,
    info_iter: Option<DenseNodeInfoIter<'a>>,
}

impl<'a> DenseRawNodeIter<'a> {
    pub(crate) fn new(osmdense: &'a osmformat::DenseNodes) -> DenseRawNodeIter<'a> {
        let info_iter = Some(DenseNodeInfoIter::new(osmdense.denseinfo.get_or_default()));
        DenseRawNodeIter {
            dids: osmdense.id.iter(),
            cid: 0,
            dlats: osmdense.lat.iter(),
            clat: 0,
            dlons: osmdense.lon.iter(),
            clon: 0,
            keys_vals_slice: osmdense.keys_vals.as_slice(),
            keys_vals_index: 0,
            info_iter,
        }
    }
}

impl<'a> Iterator for DenseRawNodeIter<'a> {
    type Item = DenseRawNode<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.dids.next(),
            self.dlats.next(),
            self.dlons.next(),
            self.info_iter.as_mut().and_then(|iter| iter.next()),
        ) {
            (Some(did), Some(dlat), Some(dlon), info) => {
                self.cid += *did;
                self.clat += *dlat;
                self.clon += *dlon;

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

                Some(DenseRawNode {
                    id: self.cid,
                    lat: self.clat,
                    lon: self.clon,
                    keys_vals_indices: &self.keys_vals_slice[start_index..end_index],
                    info: info.unwrap_or_default(),
                })
            }
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.dids.size_hint()
    }
}

impl<'a> ExactSizeIterator for DenseRawNodeIter<'a> {}

/// An iterator over dense nodes info. It decodes the delta encoded values.
#[derive(Clone, Debug)]
pub struct DenseNodeInfoIter<'a> {
    versions: std::slice::Iter<'a, i32>,
    dtimestamps: std::slice::Iter<'a, i64>, // deltas
    ctimestamp: i64,
    dchangesets: std::slice::Iter<'a, i64>, // deltas
    cchangeset: i64,
    duids: std::slice::Iter<'a, i32>, // deltas
    cuid: i32,
    duser_sids: std::slice::Iter<'a, i32>, // deltas
    cuser_sid: i32,
    visible: std::slice::Iter<'a, bool>,
}

impl<'a> DenseNodeInfoIter<'a> {
    fn new(info: &'a osmformat::DenseInfo) -> DenseNodeInfoIter<'a> {
        DenseNodeInfoIter {
            versions: info.version.iter(),
            dtimestamps: info.timestamp.iter(),
            ctimestamp: 0,
            dchangesets: info.changeset.iter(),
            cchangeset: 0,
            duids: info.uid.iter(),
            cuid: 0,
            duser_sids: info.user_sid.iter(),
            cuser_sid: 0,
            visible: info.visible.iter(),
        }
    }
}

impl<'a> Iterator for DenseNodeInfoIter<'a> {
    type Item = osmformat::Info;

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.versions.next(),
            self.dtimestamps.next(),
            self.dchangesets.next(),
            self.duids.next(),
            self.duser_sids.next(),
            self.visible.next(),
        ) {
            (
                Some(&version),
                Some(dtimestamp),
                Some(dchangeset),
                Some(duid),
                Some(duser_sid),
                visible_opt,
            ) => {
                self.ctimestamp += *dtimestamp;
                self.cchangeset += *dchangeset;
                self.cuid += *duid;
                self.cuser_sid += *duser_sid;
                Some(osmformat::Info {
                    version: Some(version),
                    timestamp: Some(self.ctimestamp),
                    changeset: Some(self.cchangeset),
                    uid: Some(self.cuid),
                    user_sid: Some(self.cuser_sid as u32),
                    visible: Some(*visible_opt.unwrap_or(&true)),
                    // protobuf uses some special fields we don't care about
                    ..Default::default()
                })
            }
            _ => None,
        }
    }
}

/// An iterator over the tags of a node. It returns a pair of indices (key and value) to the
/// stringtable of the current [`PrimitiveBlock`](crate::block::PrimitiveBlock).
#[derive(Clone, Debug)]
pub struct DenseRawTagIter<'a> {
    keys_vals_indices: std::slice::Iter<'a, i32>,
}

//TODO return Result
impl<'a> Iterator for DenseRawTagIter<'a> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.keys_vals_indices.next(), self.keys_vals_indices.next()) {
            (Some(&key_index), Some(&val_index)) => Some((key_index as u32, val_index as u32)),
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.keys_vals_indices.len() / 2;
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for DenseRawTagIter<'a> {}
