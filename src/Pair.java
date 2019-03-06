class Pair<FirstType, SecondType> {
    public final FirstType first;
    public final SecondType second;
    public Pair(FirstType f, SecondType s) {
        this.first = f; this.second = s;
    }

    @Override
    public String toString() {
        return this.first.toString() + " " + this.second.toString();
    }
}
