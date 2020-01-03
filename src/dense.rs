//! Iterate over the dense nodes in a `PrimitiveGroup`

use error::Result;
use proto::osmformat;
use block::str_from_stringtable;
use std;


//TODO Add getter functions for id, version, uid, ...
/// An OpenStreetMap node element from a compressed array of dense nodes (See [OSM wiki](http://wiki.openstreetmap.org/wiki/Node)).
#[derive(Clone, Debug)]
pub struct DenseNode<'a> {
    block: &'a osmformat::PrimitiveBlock,

    /// The node id. It should be unique between nodes and might be negative to indicate
    /// that the element has not yet been uploaded to a server.
    pub id: i64,
    /// The version of this element.
    pub version: i32,
    timestamp: i64,
    /// The changeset id.
    pub changeset: i64,
    /// The user id.
    pub uid: i32,
    user_sid: i32,
    lat: i64,
    lon: i64,
    keys_vals_indices: &'a [i32],
}

impl<'a> DenseNode<'a> {
    /// Returns the user name.
    pub fn user(&self) -> Result<&'a str> {
        str_from_stringtable(self.block, self.user_sid as usize)
    }

    /// Returns the latitude coordinate in degrees.
    pub fn lat(&self) -> f64 {
        1e-9 * self.nano_lat() as f64
    }

    /// Returns the latitude coordinate in nanodegrees (10⁻⁹).
    pub fn nano_lat(&self) -> i64 {
        self.block.get_lat_offset() + i64::from(self.block.get_granularity()) * self.lat
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
        self.block.get_lon_offset() + i64::from(self.block.get_granularity()) * self.lon
    }

    /// Returns the longitude coordinate in decimicrodegrees (10⁻⁷).
    pub fn decimicro_lon(&self) -> i32 {
        (self.nano_lon() / 100) as i32
    }

    /// Returns the time stamp in milliseconds since the epoch.
    pub fn milli_timestamp(&self) -> i64 {
        self.timestamp * i64::from(self.block.get_date_granularity())
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
    /// `PrimitiveBlock`.
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
    dids: std::slice::Iter<'a, i64>, // deltas
    cid: i64, // current id
    versions: std::slice::Iter<'a, i32>,
    dtimestamps: std::slice::Iter<'a, i64>, // deltas
    ctimestamp: i64,
    dchangesets: std::slice::Iter<'a, i64>, // deltas
    cchangeset: i64,
    duids: std::slice::Iter<'a, i32>, // deltas
    cuid: i32,
    duser_sids: std::slice::Iter<'a, i32>, // deltas
    cuser_sid: i32,
    dlats: std::slice::Iter<'a, i64>, // deltas
    clat: i64,
    dlons: std::slice::Iter<'a, i64>, // deltas
    clon: i64,
    keys_vals_slice: &'a [i32],
    keys_vals_index: usize,
}

impl<'a> DenseNodeIter<'a> {
    pub(crate) fn new(block: &'a osmformat::PrimitiveBlock,
           osmdense: &'a osmformat::DenseNodes) -> DenseNodeIter<'a> {
        let info = osmdense.get_denseinfo();
        DenseNodeIter {
            block,
            dids: osmdense.get_id().iter(),
            cid: 0,
            versions: info.get_version().iter(),
            dtimestamps: info.get_timestamp().iter(),
            ctimestamp: 0,
            dchangesets: info.get_changeset().iter(),
            cchangeset: 0,
            duids: info.get_uid().iter(),
            cuid: 0,
            duser_sids: info.get_user_sid().iter(),
            cuser_sid: 0,
            dlats: osmdense.get_lat().iter(),
            clat: 0,
            dlons: osmdense.get_lon().iter(),
            clon: 0,
            keys_vals_slice: osmdense.get_keys_vals(),
            keys_vals_index: 0,
        }
    }

    pub(crate) fn empty(block: &'a osmformat::PrimitiveBlock) -> DenseNodeIter<'a> {
        DenseNodeIter {
            block,
            dids: [].iter(),
            cid: 0,
            versions: [].iter(),
            dtimestamps: [].iter(),
            ctimestamp: 0,
            dchangesets: [].iter(),
            cchangeset: 0,
            duids: [].iter(),
            cuid: 0,
            duser_sids: [].iter(),
            cuser_sid: 0,
            dlats: [].iter(),
            clat: 0,
            dlons: [].iter(),
            clon: 0,
            keys_vals_slice: &[],
            keys_vals_index: 0,
        }
    }
}


impl<'a> Iterator for DenseNodeIter<'a> {
    type Item = DenseNode<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.dids.next(),
               self.versions.next(),
               self.dtimestamps.next(),
               self.dchangesets.next(),
               self.duids.next(),
               self.duser_sids.next(),
               self.dlats.next(),
               self.dlons.next()) {
            (Some(did),
             Some(version),
             Some(dtimestamp),
             Some(dchangeset),
             Some(duid),
             Some(duser_sid),
             Some(dlat),
             Some(dlon)) => {
                self.cid += *did;
                self.ctimestamp += *dtimestamp;
                self.cchangeset += *dchangeset;
                self.cuid += *duid;
                self.cuser_sid += *duser_sid;
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

                Some(DenseNode {
                    block: self.block,
                    id: self.cid,
                    version: *version,
                    timestamp: self.ctimestamp,
                    changeset: self.cchangeset,
                    uid: self.cuid,
                    user_sid: self.cuser_sid,
                    lat: self.clat,
                    lon: self.clon,
                    keys_vals_indices: &self.keys_vals_slice[start_index..end_index],
                })

            },
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.dids.size_hint()
    }
}

impl<'a> ExactSizeIterator for DenseNodeIter<'a> {}

/// An iterator over the tags in a dense node.
#[derive(Clone, Debug)]
pub struct DenseTagIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    keys_vals_indices: std::slice::Iter<'a, i32>,
}

//TODO return Result
impl<'a> Iterator for DenseTagIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.keys_vals_indices.next(), self.keys_vals_indices.next()) {
            (Some(&key_index), Some(&val_index)) => {
                let k_res = str_from_stringtable(self.block, key_index as usize);
                let v_res = str_from_stringtable(self.block, val_index as usize);
                if let (Ok(k), Ok(v)) = (k_res, v_res) {
                    Some((k, v))
                } else {
                    None
                }
            },
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.keys_vals_indices.len() / 2;
        (len, Some(len))
    }
}

impl<'a> ExactSizeIterator for DenseTagIter<'a> {}

/// An iterator over the tags of a node. It returns a pair of indices (key and value) to the
/// stringtable of the current `PrimitiveBlock`.
#[derive(Clone, Debug)]
pub struct DenseRawTagIter<'a> {
    keys_vals_indices: std::slice::Iter<'a, i32>,
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
