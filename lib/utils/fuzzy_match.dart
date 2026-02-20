/// Returns true if [needle]'s characters appear in [haystack] in order
/// (with possible gaps). Case-insensitive. Empty needle matches everything.
bool fuzzyMatch(String needle, String haystack) {
  if (needle.isEmpty) return true;
  final n = needle.toLowerCase();
  final h = haystack.toLowerCase();
  int hi = 0;
  for (var i = 0; i < n.length; i++) {
    final idx = h.indexOf(n[i], hi);
    if (idx == -1) return false;
    hi = idx + 1;
  }
  return true;
}
