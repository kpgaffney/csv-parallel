#[cfg(test)]
mod tests {
    use csv_parallel::partition_csv;

    #[test]
    fn test_small_input() {
        partition_csv("tests/test_input_output/test_input.txt", 4, 16);
    }
}
