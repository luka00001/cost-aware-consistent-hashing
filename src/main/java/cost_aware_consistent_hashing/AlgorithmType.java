package cost_aware_consistent_hashing;

/*
* What algorithm to use when deciding which "host" to map a task to
*/
public enum AlgorithmType {
    MODULO, CONSISTENT_SINGULAR, CONSISTENT, BOUNDED_LOAD, REHASH, BOUNDED_ELAPSED, MIN_CHOICE, MIN_CHOICE_ELAPSED,
}
