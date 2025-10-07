from typing import List, Tuple


def _sift_up(heap: List[Tuple[float, int, float, float]], idx: int) -> None:
    while idx > 0:
        parent = (idx - 1) // 2
        if heap[parent][0] <= heap[idx][0]:
            break
        heap[parent], heap[idx] = heap[idx], heap[parent]
        idx = parent


def _sift_down(heap: List[Tuple[float, int, float, float]], idx: int) -> None:
    size = len(heap)
    while True:
        left = idx * 2 + 1
        right = idx * 2 + 2
        smallest = idx
        if left < size and heap[left][0] < heap[smallest][0]:
            smallest = left
        if right < size and heap[right][0] < heap[smallest][0]:
            smallest = right
        if smallest == idx:
            break
        heap[idx], heap[smallest] = heap[smallest], heap[idx]
        idx = smallest


def top_k_by_tip_percentage(heap: List[Tuple[float, int, float, float]], k: int, item: Tuple[float, int, float, float]) -> None:
    """
    Maintain a min-heap of size at most k keyed by tip percentage.
    Each item is a tuple: (tip_pct, trip_id, fare_amount, tip_amount)
    """
    if len(heap) < k:
        heap.append(item)
        _sift_up(heap, len(heap) - 1)
        return
    # If new item better than heap root, replace root
    if item[0] > heap[0][0]:
        heap[0] = item
        _sift_down(heap, 0)


