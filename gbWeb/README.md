# geoBoundaries

<img src="images/globe.svg" alt="geoBoundaries globe" width="140" align="right">

_A free, open database of political administrative boundaries—global coverage, open licenses, built by the community and the William & Mary geoLab._

Since 2016, the **geoBoundaries Global Database of Political Administrative Boundaries** has tracked roughly **one million** administrative units across **200+ entities** (including all UN member states). Every release ships with consistent metadata and files you can actually use in production—**Shapefiles**, **GeoJSON**, static **images**, and API access—so you can drop boundaries straight into analysis, web maps, and applications without gymnastics.

<a href="YOUR_DOWNLOAD_URL" target="_blank" rel="noopener"
   style="display:inline-block;background:#115740;color:#fff;text-decoration:none;
          padding:.9em 1.6em;border-radius:9999px;font-weight:700;letter-spacing:.02em;
          box-shadow:0 8px 24px rgba(0,0,0,.08);transition:transform .05s ease-out">
  ⬇︎ Download the data here
</a>

## What you get

You get standardized, versioned boundary layers across multiple administrative levels (country, state/province, county/district). Each download includes a `meta.txt` detailing provenance, license, and any attribution requirements. Browse, filter, and download from **[www.geoboundaries.org](https://www.geoboundaries.org)**, or see the **[Documentation](https://github.com/wmgeolab/geoBoundaries)** for format notes and API details.

## Quick start

Pick a country and admin level on the site. Download the format you need. Use it the way you already work.

**Python / GeoPandas:**
~~~python
import geopandas as gpd
gdf = gpd.read_file("path/to/geoboundaries.geojson")  # or .shp
gdf.plot(figsize=(8,8))
~~~

**JavaScript / Web map (Leaflet example):**
~~~html
<script>
fetch('path/to/geoboundaries.geojson')
  .then(r => r.json())
  .then(geojson => L.geoJSON(geojson).addTo(map));
</script>
~~~

If you hit an edge case, the answer is almost always in the layer’s `meta.txt`.

## Licensing, attribution & citation

<details open>
  <summary><strong>Open data you can actually use — here’s exactly how to acknowledge and cite</strong></summary>

<p class="tip"><strong>Minimum acknowledgement most users need:</strong><br>
Data: <strong>geoBoundaries</strong>, CC BY 4.0 — https://www.geoboundaries.org
</p>

**Licenses.** geoBoundaries layers are open-licensed (typically CC BY 4.0). Commercial, non-commercial, and academic use are fine. The non-negotiable is **acknowledgement**. If a layer needs specific wording, it’s spelled out in the `meta.txt` packaged with your download—use that verbatim.

**On the web.** Put the name **“geoBoundaries”** linked to **https://www.geoboundaries.org** somewhere visible near your map or in site credits.

**In apps, video, talks, or anything non-web.** Include **“geoBoundaries — www.geoboundaries.org”** in a visible place (About, Help, Credits, end titles). If users can’t click, write the full URL.

**If you write about it.** Cite the peer-reviewed article:

Runfola, D. <em>et al.</em> (2020). <em>geoBoundaries: A global database of political administrative boundaries.</em> PLOS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866

Short form for in-text: **Runfola et al., 2020 (PLOS ONE, e0231866)**. Adapt to your style guide if needed.

**Not sure?** Check the layer’s `meta.txt` or follow Creative Commons best-practice attribution. Keep it simple: acknowledge clearly, link back, and cite the paper when you publish analysis.
</details>

## Why people use geoBoundaries

You get global coverage that’s actually maintained, clean metadata, and formats that slot into common tools without custom parsers. The community improves coverage and quality; we track impact through citations and acknowledgements. If you do something interesting with the data, we want to see it — **team@geoboundaries.org**.

## Need help or want to contribute?

If something looks off, open an issue or pull request in the **[Documentation / GitHub repo](https://github.com/wmgeolab/geoBoundaries)**. If you have a dataset to contribute, include provenance and license details up front; it saves everyone time.

<small>© geoBoundaries. Licensing is per-layer as documented in each `meta.txt`. Please acknowledge us—it’s how the community, funders, and peers track impact.</small>
