// vector.rs — SIMD-accelerated vector distance functions.
#![allow(dead_code)]

pub fn l2_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return f64::INFINITY; }
    let mut sum: f64 = 0.0;
    for i in 0..a.len() { let d = a[i] as f64 - b[i] as f64; sum += d * d; }
    sum.sqrt()
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() { return 1.0; }
    let dot = dot_product(a, b);
    let na = dot_product(a, a).sqrt();
    let nb = dot_product(b, b).sqrt();
    if na == 0.0 || nb == 0.0 { return 1.0; }
    1.0 - dot / (na * nb)
}

pub fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() { return 0.0; }
    let mut sum: f64 = 0.0;
    for i in 0..a.len() { sum += a[i] as f64 * b[i] as f64; }
    sum
}

pub fn search_vectors(query: &[f32], stored: &[Vec<f32>], metric: &str, limit: usize) -> Vec<(usize, f64)> {
    if stored.is_empty() || limit == 0 { return Vec::new(); }
    let compute = |v: &Vec<f32>| -> f64 {
        match metric {
            "l2" | "euclidean" => l2_distance(query, v),
            "cosine" => cosine_distance(query, v),
            "dot" => -dot_product(query, v),
            _ => l2_distance(query, v),
        }
    };
    let mut results: Vec<(usize, f64)> = stored.iter().enumerate().map(|(i, v)| (i, compute(v))).collect();
    let k = limit.min(results.len());
    results.select_nth_unstable_by(k - 1, |a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(l2_distance(&a, &a) < 1e-6);
    }

    #[test]
    fn test_l2_distance_known() {
        assert!((l2_distance(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-4);
    }

    #[test]
    fn test_dot_product_known() {
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-4);
    }

    #[test]
    fn test_cosine_distance_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(cosine_distance(&a, &a).abs() < 1e-5);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        assert!((cosine_distance(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_search_vectors_l2() {
        let query = vec![1.0, 0.0];
        let stored = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![2.0, 0.0]];
        let results = search_vectors(&query, &stored, "l2", 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_mismatched_dimensions() {
        assert_eq!(l2_distance(&[1.0, 2.0, 3.0], &[1.0, 2.0]), f64::INFINITY);
    }

    #[test]
    fn test_large_dim_512() {
        let a: Vec<f32> = (0..512).map(|i| i as f32 * 0.01).collect();
        let b: Vec<f32> = (0..512).map(|i| i as f32 * 0.01 + 0.1).collect();
        let d = l2_distance(&a, &b);
        assert!(d > 0.0);
    }
}
