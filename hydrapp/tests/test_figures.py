"""Actual HAT demo discovery, paired assets, metadata and HTTP routes."""
import json
from pathlib import Path
import tempfile
import unittest

from hydrapp.app import create_app
from shybox.publishing_toolkit import LegacyFigureCatalog


class FigureTests(unittest.TestCase):
    def setUp(self):
        self.client=create_app('hydrapp/config.demo.json').test_client()

    def test_portable_archive_contains_expected_products(self):
        data=self.client.get('/api/figures').json
        self.assertEqual(len(data['products']),23);self.assertEqual(data['issues'],[])
        self.assertEqual(sum(p['kind']=='maps' for p in data['products']),10)
        self.assertEqual(sum(p['kind']=='time_series' for p in data['products']),13)
        self.assertEqual({p['window'] for p in data['products'] if p['kind']=='maps'}, {'3H','6H','12H','24H','last'})

    def test_source_initialization_is_not_inferred_from_publication_directory(self):
        data=self.client.get('/api/figures').json['products']
        product=next(p for p in data if p['group']=='nwp_deterministic_ecmwf0100' and p['section']=='Montecchio')
        self.assertEqual(product['publication_time'],'2026-10-21T03:00:00Z')
        source=next(s for s in product['sources'] if s['label']=='nwp')
        self.assertEqual(source['run_time'],'2026-10-20T05:00:00Z')

    def test_image_metadata_download_and_invalid_assets(self):
        product=self.client.get('/api/figures').json['products'][0]
        with self.client.get(product['image_url']) as response:
            self.assertEqual(response.status_code,200);self.assertTrue(response.data.startswith(b'\x89PNG'))
        with self.client.get(product['metadata_url']+'?download=1') as response:
            self.assertEqual(response.status_code,200);self.assertIn('attachment',response.headers['Content-Disposition'])
            self.assertIsInstance(response.json,dict)
        self.assertEqual(self.client.get('/api/figures/unknown/image').status_code,404)
        self.assertEqual(self.client.get(f"/api/figures/{product['id']}/invalid").status_code,404)

    def test_pages_and_demo_home(self):
        self.assertEqual(self.client.get('/').headers['Location'],'/figures')
        for path in ['/figures','/figures/time-series','/maps','/runs','/time-series','/monitoring']:
            with self.subTest(path=path):self.assertEqual(self.client.get(path).status_code,200)

    def test_missing_pair_and_external_symlinks_are_not_published(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'archive';group=root/'maps'/'rain'/'2026102104';group.mkdir(parents=True)
            (group/'unpaired.json').write_text('{}')
            external=Path(folder)/'outside.json';external.write_text('{}')
            (group/'external.json').symlink_to(external)
            (group/'external.png').write_bytes(b'outside')
            data=LegacyFigureCatalog(root).discover()
            self.assertEqual(data['products'],[]);self.assertEqual(len(data['issues']),1)

    def test_ram_and_ruler_demo_snapshot(self):
        data=self.client.get('/api/monitoring/memory').json
        self.assertIn(data['status'],['available','stale'])
        self.assertTrue(data['targets']);self.assertEqual(data['ruler']['source'],'c-hydro/ruler')
        with self.client.get('/api/monitoring/memory/plot') as response:self.assertEqual(response.status_code,200)


if __name__=='__main__':unittest.main()
