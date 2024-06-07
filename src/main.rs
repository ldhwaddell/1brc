use fast_float;
use fxhash::FxHashMap as HashMap;
use memchr::memchr;
use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::fmt::Display;
use std::fs::File;
use std::str;
use std::thread;

#[derive(Debug)]
struct Stats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            count: 0,
        }
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let avg = self.sum / (self.count as f64);
        write!(f, "{:.1}/{avg:.1}/{:.1}", self.min, self.max)
    }
}

impl Stats {
    fn update(&mut self, other: f64) {
        self.min = self.min.min(other);
        self.max = self.max.max(other);
        self.sum += other;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

fn chunk(mmap: &Mmap, cores: &usize) -> Vec<(usize, usize)> {
    let chunk_size = mmap.len() / *cores;
    let mut start = 0;
    let mut chunks: Vec<(usize, usize)> = Vec::new();

    for _ in 0..*cores {
        let end = (start + chunk_size).min(mmap.len());
        let next_new_line = match memchr(b'\n', &mmap[end..]) {
            Some(index) => index,
            None => mmap.len(),
        };

        let end = end + next_new_line;

        chunks.push((start, end));
        start = end + 1;
    }

    chunks
}

fn parse_chunk<'a>(start: &usize, end: &usize, mmap: &'a [u8]) -> HashMap<&'a str, Stats> {
    let mut hashmap: HashMap<&'a str, Stats> = HashMap::default();

    if let Some(slice) = mmap.get(*start..*end) {
        let chunk_str = unsafe { str::from_utf8_unchecked(slice) };
        for line in chunk_str.lines() {
            let (place, temp) = line.split_once(";").unwrap();
            let temp: f64 = fast_float::parse(temp).unwrap();
            // Get the key, or adds a default if there is not one, and then updates it
            hashmap.entry(place).or_default().update(temp);
        }
    }

    hashmap
}

fn merge<'a>(a: &mut HashMap<&'a str, Stats>, b: &HashMap<&'a str, Stats>) {
    for (k, v) in b {
        a.entry(k).or_default().merge(v);
    }
}

fn main() {
    let path = "./data/measurements.txt";
    let cores: usize = thread::available_parallelism().unwrap().into();

    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let chunks = chunk(&mmap, &cores);

    let parsed_chunks: Vec<_> = chunks
        .par_iter()
        .map(|(start, end)| parse_chunk(start, end, &mmap))
        .collect();

    println!("{:?}", parsed_chunks);

    let stats: HashMap<&str, Stats> =
        parsed_chunks
            .into_iter()
            .fold(Default::default(), |mut a, b| {
                merge(&mut a, &b);
                a
            });

    let mut stats_vec: Vec<_> = stats.into_iter().collect();
    stats_vec.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (i, (name, state)) in stats_vec.into_iter().enumerate() {
        if i == 0 {
            println!("{name}={state}");
        } else {
            println!(", {name}={state}");
        }
    }
}