import unittest
from src.nodes.queue_node import ConsistentHashRing

class TestConsistentHashing(unittest.TestCase):
    def test_node_distribution(self):
        nodes = ["node1:8001", "node2:8002", "node3:8003"]
        ring = ConsistentHashRing(nodes, virtual_nodes=100)
        
        # Pastikan tugas jatuh ke salah satu node yang valid
        target1 = ring.get_node("tugas_A")
        self.assertIn(target1, nodes)
        
    def test_consistency(self):
        nodes = ["node1:8001", "node2:8002", "node3:8003"]
        ring = ConsistentHashRing(nodes, virtual_nodes=100)
        
        # Hash untuk nama tugas yang sama harus selalu jatuh ke Node yang sama
        target_pertama = ring.get_node("fixed_job_123")
        target_kedua = ring.get_node("fixed_job_123")
        
        self.assertEqual(target_pertama, target_kedua)

if __name__ == '__main__':
    unittest.main()