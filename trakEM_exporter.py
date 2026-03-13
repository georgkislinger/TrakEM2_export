from __future__ import print_function

from ij import ImagePlus, ImageStack, IJ
from ini.trakem2 import Project
from ini.trakem2.display import Patch, Display
from ij.io import FileSaver
from ij import ImagePlus
from ij.process import ImageProcessor
from java.awt import Color
from java.io import File, FileOutputStream, BufferedOutputStream
from java.lang import Runnable, Runtime
from java.nio import ByteBuffer, ByteOrder
from java.util import ArrayList, Collections
from java.util.concurrent import Executors, TimeUnit
from ij.gui import GenericDialog
import json
import math
import os
import sys
import traceback


DEFAULT_VOXEL_X_NM = 4.0
DEFAULT_VOXEL_Y_NM = 4.0
DEFAULT_VOXEL_Z_NM = 100.0
DEFAULT_SCALE = 1.0
DEFAULT_TILE_SIZE = 1024
OME_ZARR_VERSION = "0.4"


def log(msg):
    print(msg)
    try:
        IJ.log(str(msg))
    except:
        pass


def fail(msg):
    IJ.error("TrakEM2 export", str(msg))
    raise RuntimeError(msg)


def safe_float(text, default_value):
    if text is None:
        return default_value
    text = str(text).strip()
    if text == "":
        return default_value
    try:
        return float(text)
    except:
        return default_value


def safe_int(text, default_value):
    if text is None:
        return default_value
    text = str(text).strip()
    if text == "":
        return default_value
    try:
        return int(round(float(text)))
    except:
        return default_value


def ensure_dir(path):
    f = File(path)
    if not f.exists():
        if not f.mkdirs():
            fail("Could not create directory: %s" % path)


def write_text_file(path, text):
    parent = File(path).getParentFile()
    if parent is not None:
        ensure_dir(parent.getAbsolutePath())
    fh = open(path, "w")
    try:
        fh.write(text)
    finally:
        fh.close()


def json_dump_pretty(data):
    return json.dumps(data, indent=4, sort_keys=True)


def bytes_per_pixel_for_mode(mode_name):
    if mode_name == "8bit GRAY":
        return 1
    if mode_name == "16bit GRAY":
        return 2
    if mode_name == "32bit COLOR":
        return 3
    if mode_name == "8bit COLOR":
        return 1
    return 1


def color_mode_constant(mode_name):
    if mode_name == "8bit GRAY":
        return ImagePlus.GRAY8
    if mode_name == "16bit GRAY":
        return ImagePlus.GRAY16
    if mode_name == "8bit COLOR":
        return ImagePlus.COLOR_256
    if mode_name == "32bit COLOR":
        return ImagePlus.COLOR_RGB
    fail("Unsupported color mode: %s" % mode_name)


def background_color_from_name(name):
    if name == "black":
        return Color.black
    if name == "white":
        return Color.white
    fail("Unsupported background color: %s" % name)


def numeric_background_fill(mode_name, background_name):
    white = background_name == "white"
    if mode_name == "8bit GRAY":
        return 255 if white else 0
    if mode_name == "16bit GRAY":
        return 65535 if white else 0
    if mode_name == "8bit COLOR":
        return 255 if white else 0
    if mode_name == "32bit COLOR":
        return None
    return 0


def auto_mip_count(width, height):
    max_dim = max(int(width), int(height))
    count = 0
    w = int(width)
    h = int(height)
    while max_dim >= 512:
        w = max(1, int(math.ceil(w / 2.0)))
        h = max(1, int(math.ceil(h / 2.0)))
        count += 1
        max_dim = max(w, h)
    return count


def downsample_xy(ip):
    new_w = max(1, int(math.ceil(ip.getWidth() / 2.0)))
    new_h = max(1, int(math.ceil(ip.getHeight() / 2.0)))
    dup = ip.duplicate()
    dup.setInterpolationMethod(ImageProcessor.BILINEAR)
    return dup.resize(new_w, new_h, True)


def render_flat_layer(layer, bounds, scale, color_mode, background_color):
    tiles = layer.getDisplayables(Patch)
    return Patch.makeFlatImage(
        color_mode,
        layer,
        bounds,
        scale,
        tiles,
        background_color,
        True
    )


def processor_crop(ip, x, y, w, h):
    ip.setRoi(x, y, w, h)
    return ip.crop()


def padded_tile(ip, x, y, tile_size, mode_name, background_color_name, background_color):
    src_w = min(tile_size, ip.getWidth() - x)
    src_h = min(tile_size, ip.getHeight() - y)
    crop = processor_crop(ip, x, y, src_w, src_h)
    if src_w == tile_size and src_h == tile_size:
        return crop

    out = ip.createProcessor(tile_size, tile_size)
    if mode_name == "32bit COLOR":
        out.setColor(background_color)
        out.fill()
    else:
        out.setValue(float(numeric_background_fill(mode_name, background_color_name)))
        out.fill()
    out.insert(crop, 0, 0)
    return out


def save_processor_as_image(ip, path, image_format):
    imp = ImagePlus("export", ip)
    saver = FileSaver(imp)
    ok = False
    if image_format == "tif":
        ok = saver.saveAsTiff(path)
    elif image_format == "png":
        ok = saver.saveAsPng(path)
    elif image_format == "jpg":
        ok = saver.saveAsJpeg(path)
    else:
        fail("Unsupported file format: %s" % image_format)
    if not ok:
        fail("Failed to save file: %s" % path)


def shorts_to_little_endian_bytes(short_array):
    buf = ByteBuffer.allocate(len(short_array) * 2)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    for i in range(len(short_array)):
        buf.putShort(short_array[i])
    return buf.array()


def processor_to_raw_bytes(ip, mode_name):
    pixels = ip.getPixels()
    if mode_name == "8bit GRAY":
        return pixels
    if mode_name == "16bit GRAY":
        return shorts_to_little_endian_bytes(pixels)
    fail("OME-Zarr writer in this script currently supports only 8bit GRAY and 16bit GRAY")


def write_bytes(path, byte_array):
    parent = File(path).getParentFile()
    if parent is not None:
        ensure_dir(parent.getAbsolutePath())
    stream = BufferedOutputStream(FileOutputStream(path))
    try:
        stream.write(byte_array)
        stream.flush()
    finally:
        stream.close()


def zarr_dtype_for_mode(mode_name):
    if mode_name == "8bit GRAY":
        return "|u1"
    if mode_name == "16bit GRAY":
        return "<u2"
    fail("OME-Zarr writer in this script currently supports only 8bit GRAY and 16bit GRAY")


def build_omezarr_metadata(dataset_name, num_sections, base_width, base_height,
                           voxel_x_nm, voxel_y_nm, voxel_z_nm, export_scale,
                           tile_size, max_m, mode_name, background_name):
    axes = [
        {"name": "z", "type": "space", "unit": "micrometer"},
        {"name": "y", "type": "space", "unit": "micrometer"},
        {"name": "x", "type": "space", "unit": "micrometer"}
    ]

    base_x_um = (voxel_x_nm / 1000.0) / export_scale
    base_y_um = (voxel_y_nm / 1000.0) / export_scale
    base_z_um = voxel_z_nm / 1000.0

    datasets = []
    level_shapes = []
    w = int(base_width)
    h = int(base_height)

    for level in range(max_m + 1):
        datasets.append({
            "path": str(level),
            "coordinateTransformations": [
                {
                    "type": "scale",
                    "scale": [base_z_um, base_y_um * (2 ** level), base_x_um * (2 ** level)]
                }
            ]
        })
        level_shapes.append((num_sections, h, w))
        if level < max_m:
            w = max(1, int(math.ceil(w / 2.0)))
            h = max(1, int(math.ceil(h / 2.0)))

    group_attrs = {
        "multiscales": [
            {
                "version": OME_ZARR_VERSION,
                "name": dataset_name,
                "axes": axes,
                "datasets": datasets
            }
        ]
    }

    zarr_arrays = []
    dtype = zarr_dtype_for_mode(mode_name)
    fill_value = numeric_background_fill(mode_name, background_name)
    for level, shape in enumerate(level_shapes):
        zarr_arrays.append({
            "level": level,
            "shape": [int(shape[0]), int(shape[1]), int(shape[2])],
            "zarray": {
                "chunks": [1, tile_size, tile_size],
                "compressor": None,
                "dimension_separator": "/",
                "dtype": dtype,
                "fill_value": fill_value,
                "filters": None,
                "order": "C",
                "shape": [int(shape[0]), int(shape[1]), int(shape[2])],
                "zarr_format": 2
            }
        })
    return group_attrs, zarr_arrays, level_shapes


def write_omezarr_skeleton(store_path, dataset_name, num_sections, base_width, base_height,
                           voxel_x_nm, voxel_y_nm, voxel_z_nm, export_scale,
                           tile_size, max_m, mode_name, background_name, write_array_dimensions):
    ensure_dir(store_path)
    write_text_file(os.path.join(store_path, ".zgroup"), json_dump_pretty({"zarr_format": 2}))

    group_attrs, zarr_arrays, level_shapes = build_omezarr_metadata(
        dataset_name, num_sections, base_width, base_height,
        voxel_x_nm, voxel_y_nm, voxel_z_nm, export_scale,
        tile_size, max_m, mode_name, background_name
    )
    write_text_file(os.path.join(store_path, ".zattrs"), json_dump_pretty(group_attrs))

    for array_meta in zarr_arrays:
        array_dir = os.path.join(store_path, str(array_meta["level"]))
        ensure_dir(array_dir)
        write_text_file(os.path.join(array_dir, ".zarray"), json_dump_pretty(array_meta["zarray"]))
        zattrs = {}
        if write_array_dimensions:
            zattrs["_ARRAY_DIMENSIONS"] = ["z", "y", "x"]
        write_text_file(os.path.join(array_dir, ".zattrs"), json_dump_pretty(zattrs))

    return level_shapes


def write_vast_vsvi(vast_root, dataset_name, comment_text, first_section_number, last_section_number,
                    num_sections, base_width, base_height, tile_size, tile_format, max_m,
                    voxel_x_nm, voxel_y_nm, voxel_z_nm, export_scale, mode_name):
    rows = int(math.ceil(base_height / float(tile_size)))
    cols = int(math.ceil(base_width / float(tile_size)))

    bytes_per_pixel = bytes_per_pixel_for_mode(mode_name)
    if bytes_per_pixel not in (1, 3):
        fail("VASTlite export from this script is intended for 8-bit grayscale or RGB imagery. 16-bit grayscale is not a good VAST target.")

    vsvi = {
        "Comment": comment_text,
        "ServerType": "imagetiles",
        "SourceFileNameTemplate": ".\\mip0\\slice%04d\\%04d_tr%d-tc%d." + tile_format,
        "SourceParamSequence": "ssrc",
        "SourceMinS": int(first_section_number),
        "SourceMaxS": int(last_section_number),
        "SourceMinR": 1,
        "SourceMaxR": int(rows),
        "SourceMinC": 1,
        "SourceMaxC": int(cols),
        "MipMapFileNameTemplate": ".\\mip%d\\slice%04d\\%04d_tr%d-tc%d." + tile_format,
        "MipMapParamSequence": "mssrc",
        "SourceMinM": 1,
        "SourceMaxM": int(max_m),
        "SourceTileSizeX": int(tile_size),
        "SourceTileSizeY": int(tile_size),
        "SourceBytesPerPixel": int(bytes_per_pixel),
        "MissingImagePolicy": "nearest",
        "TargetDataSizeX": int(base_width),
        "TargetDataSizeY": int(base_height),
        "TargetDataSizeZ": int(num_sections),
        "OffsetX": 0,
        "OffsetY": 0,
        "OffsetZ": 0,
        "OffsetMip": 0,
        "TargetVoxelSizeXnm": float(voxel_x_nm) / float(export_scale),
        "TargetVoxelSizeYnm": float(voxel_y_nm) / float(export_scale),
        "TargetVoxelSizeZnm": float(voxel_z_nm),
        "TargetLayerName": comment_text
    }

    write_text_file(os.path.join(vast_root, dataset_name + ".vsvi"), json_dump_pretty(vsvi))


class SectionExportTask(Runnable):

    def __init__(self, z_rel_index, section_number, layer, pre_rendered_ip=None):
        self.z_rel_index = int(z_rel_index)
        self.section_number = int(section_number)
        self.layer = layer
        self.pre_rendered_ip = pre_rendered_ip

    def run(self):
        try:
            if self.pre_rendered_ip is None:
                ip = render_flat_layer(
                    self.layer,
                    CONFIG["bounds"],
                    CONFIG["scale"],
                    CONFIG["color_mode"],
                    CONFIG["background_color"]
                )
            else:
                ip = self.pre_rendered_ip

            if CONFIG["write_flat_images"]:
                flat_name = CONFIG["base_name"] + str(self.section_number).zfill(4) + "." + CONFIG["flat_format"]
                flat_path = os.path.join(CONFIG["flat_output_dir"], flat_name)
                save_processor_as_image(ip, flat_path, CONFIG["flat_format"])

            if CONFIG["write_vast"] or CONFIG["write_omezarr"]:
                current_ip = ip
                for level in range(CONFIG["max_m"] + 1):
                    if CONFIG["write_vast"]:
                        if level == 0:
                            level_dir = os.path.join(CONFIG["vast_root"], "mip0", "slice%04d" % self.section_number)
                        else:
                            level_dir = os.path.join(CONFIG["vast_root"], "mip%d" % level, "slice%04d" % self.section_number)
                        ensure_dir(level_dir)

                    rows = int(math.ceil(current_ip.getHeight() / float(CONFIG["tile_size"])))
                    cols = int(math.ceil(current_ip.getWidth() / float(CONFIG["tile_size"])))

                    for row in range(rows):
                        y = row * CONFIG["tile_size"]
                        for col in range(cols):
                            x = col * CONFIG["tile_size"]
                            tile_ip = padded_tile(
                                current_ip,
                                x,
                                y,
                                CONFIG["tile_size"],
                                CONFIG["mode_name"],
                                CONFIG["background_name"],
                                CONFIG["background_color"]
                            )

                            if CONFIG["write_vast"]:
                                tile_name = "%04d_tr%d-tc%d.%s" % (
                                    self.section_number,
                                    row + 1,
                                    col + 1,
                                    CONFIG["vast_tile_format"]
                                )
                                tile_path = os.path.join(level_dir, tile_name)
                                save_processor_as_image(tile_ip, tile_path, CONFIG["vast_tile_format"])

                            if CONFIG["write_omezarr"]:
                                chunk_path = os.path.join(
                                    CONFIG["omezarr_root"],
                                    str(level),
                                    str(self.z_rel_index),
                                    str(row),
                                    str(col)
                                )
                                raw = processor_to_raw_bytes(tile_ip, CONFIG["mode_name"])
                                write_bytes(chunk_path, raw)

                    if level < CONFIG["max_m"]:
                        current_ip = downsample_xy(current_ip)

            CONFIG["completed"].add(self.section_number)
            log("Finished section %04d" % self.section_number)
        except:
            CONFIG["errors"].add(traceback.format_exc())


def build_dialog(layerset_size):
    gui = GenericDialog("Export from TrakEM2")
    gui.addStringField("Output directory:", "C:/your/path", 50)
    gui.addStringField("Dataset / filename prefix:", "section_", 30)
    gui.addStringField("Comment / layer name:", "TrakEM2 export", 40)
    gui.addChoice(
        "Export mode",
        [
            "Flat images",
            "Show stack",
            "OME-Zarr (0.4)",
            "VASTlite tiles + .vsvi",
            "OME-Zarr + VASTlite"
        ],
        "OME-Zarr + VASTlite"
    )
    gui.addChoice("Flat file format", ["tif", "png", "jpg"], "tif")
    gui.addChoice("VAST tile format", ["png", "jpg"], "png")
    gui.addChoice("Background color", ["black", "white"], "black")
    gui.addChoice("Color mode", ["8bit GRAY", "16bit GRAY", "8bit COLOR", "32bit COLOR"], "8bit GRAY")
    gui.addStringField("Scale (0 < s <= 1, blank = 1)", "1", 8)
    gui.addCheckbox("Export full stack?", True)
    gui.addSlider("First section to export", 1, layerset_size, 1)
    gui.addSlider("Last section to export", 1, layerset_size, layerset_size)
    gui.addStringField("Voxel size X (nm, blank = 4)", str(DEFAULT_VOXEL_X_NM), 8)
    gui.addStringField("Voxel size Y (nm, blank = 4)", str(DEFAULT_VOXEL_Y_NM), 8)
    gui.addStringField("Voxel size Z (nm, blank = 100)", str(DEFAULT_VOXEL_Z_NM), 8)
    gui.addStringField("Tile / chunk size XY (blank = 1024)", str(DEFAULT_TILE_SIZE), 8)
    gui.addStringField("Pyramid levels (blank = auto)", "", 8)
    gui.addStringField("Max workers (blank = all available)", "", 8)
    gui.addCheckbox("Write _ARRAY_DIMENSIONS in OME-Zarr arrays", True)
    gui.addHelp(r"github.com/georgkislinger/")
    return gui


CONFIG = {}


def main():
    projects = Project.getProjects()
    if projects is None or len(projects) == 0:
        fail("No open TrakEM2 project found")

    project = projects[0]
    layerset = project.getRootLayerSet()
    front = Display.getFront(project)
    layerset.setMinimumDimensions()

    roi_obj = None
    if front is not None:
        roi_obj = front.getRoi()
    if roi_obj is None:
        bounds = layerset.get2DBounds()
    else:
        bounds = roi_obj.getBounds()

    gui = build_dialog(layerset.size())
    gui.showDialog()
    if not gui.wasOKed():
        log("Cancelled...")
        return

    output_dir = gui.getNextString().strip()
    base_name = gui.getNextString().strip()
    comment_text = gui.getNextString().strip()
    export_mode = gui.getNextChoice()
    flat_format = gui.getNextChoice()
    vast_tile_format = gui.getNextChoice()
    background_name = gui.getNextChoice()
    mode_name = gui.getNextChoice()
    scale = safe_float(gui.getNextString(), DEFAULT_SCALE)
    full_stack = gui.getNextBoolean()
    min_sec = int(round(gui.getNextNumber()))
    max_sec = int(round(gui.getNextNumber()))
    voxel_x_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_X_NM)
    voxel_y_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_Y_NM)
    voxel_z_nm = safe_float(gui.getNextString(), DEFAULT_VOXEL_Z_NM)
    tile_size = safe_int(gui.getNextString(), DEFAULT_TILE_SIZE)
    pyramid_text = gui.getNextString().strip()
    workers_text = gui.getNextString().strip()
    write_array_dimensions = gui.getNextBoolean()

    if output_dir == "":
        fail("Please provide an output directory")
    if base_name == "":
        fail("Please provide a dataset or filename prefix")
    if comment_text == "":
        comment_text = base_name
    if scale <= 0.0 or scale > 1.0:
        fail("Scale must satisfy 0 < scale <= 1")
    if tile_size <= 0:
        fail("Tile size must be a positive integer")
    if tile_size % 16 != 0 and export_mode in ("VASTlite tiles + .vsvi", "OME-Zarr + VASTlite"):
        fail("For VASTlite, tile size should be a multiple of 16")

    total_layers = layerset.size()
    if full_stack:
        min_index = 0
        max_index = total_layers - 1
    else:
        min_index = max(0, min(total_layers - 1, min_sec - 1))
        max_index = max(0, min(total_layers - 1, max_sec - 1))
        if max_index < min_index:
            fail("Last section must be >= first section")

    layers = list(layerset.getLayers(min_index, max_index))
    if len(layers) == 0:
        fail("No layers selected for export")

    available_workers = Runtime.getRuntime().availableProcessors()
    max_workers = safe_int(workers_text, available_workers)
    if max_workers < 1:
        max_workers = 1

    write_flat_images = export_mode == "Flat images"
    show_stack = export_mode == "Show stack"
    write_omezarr = export_mode in ("OME-Zarr (0.4)", "OME-Zarr + VASTlite")
    write_vast = export_mode in ("VASTlite tiles + .vsvi", "OME-Zarr + VASTlite")

    if write_omezarr and mode_name not in ("8bit GRAY", "16bit GRAY"):
        fail("This OME-Zarr writer currently supports only 8bit GRAY and 16bit GRAY")

    if write_vast and mode_name == "16bit GRAY":
        fail("VASTlite export is best targeted to 8-bit grayscale or RGB; 16-bit grayscale is not handled here")
    if write_vast and mode_name == "8bit COLOR":
        fail("For VASTlite, prefer 32bit COLOR instead of indexed 8bit COLOR so SourceBytesPerPixel is unambiguous")

    color_mode = color_mode_constant(mode_name)
    background_color = background_color_from_name(background_name)

    first_ip = render_flat_layer(layers[0], bounds, scale, color_mode, background_color)
    base_width = int(first_ip.getWidth())
    base_height = int(first_ip.getHeight())

    user_mips = safe_int(pyramid_text, None)
    if user_mips is None:
        max_m = auto_mip_count(base_width, base_height)
    else:
        max_m = max(0, int(user_mips))
    if write_vast and max_m < 1:
        max_m = 1

    ensure_dir(output_dir)

    flat_output_dir = output_dir
    omezarr_root = os.path.join(output_dir, base_name + ".ome.zarr")
    vast_root = os.path.join(output_dir, base_name + "_vastlite")

    num_sections = len(layers)
    first_section_number = min_index + 1
    last_section_number = max_index + 1

    if write_omezarr:
        write_omezarr_skeleton(
            omezarr_root,
            comment_text,
            num_sections,
            base_width,
            base_height,
            voxel_x_nm,
            voxel_y_nm,
            voxel_z_nm,
            scale,
            tile_size,
            max_m,
            mode_name,
            background_name,
            write_array_dimensions
        )

    if write_vast:
        ensure_dir(vast_root)
        write_vast_vsvi(
            vast_root,
            base_name,
            comment_text,
            first_section_number,
            last_section_number,
            num_sections,
            base_width,
            base_height,
            tile_size,
            vast_tile_format,
            max_m,
            voxel_x_nm,
            voxel_y_nm,
            voxel_z_nm,
            scale,
            mode_name
        )

    CONFIG.clear()
    CONFIG.update({
        "bounds": bounds,
        "scale": scale,
        "background_name": background_name,
        "background_color": background_color,
        "mode_name": mode_name,
        "color_mode": color_mode,
        "base_name": base_name,
        "comment_text": comment_text,
        "flat_format": flat_format,
        "vast_tile_format": vast_tile_format,
        "tile_size": tile_size,
        "max_m": max_m,
        "write_flat_images": write_flat_images,
        "show_stack": show_stack,
        "write_omezarr": write_omezarr,
        "write_vast": write_vast,
        "flat_output_dir": flat_output_dir,
        "omezarr_root": omezarr_root,
        "vast_root": vast_root,
        "errors": Collections.synchronizedList(ArrayList()),
        "completed": Collections.synchronizedList(ArrayList())
    })

    if show_stack:
        stack = ImageStack(base_width, base_height)
        stack.addSlice(str(first_section_number).zfill(4), first_ip)
        for rel_i in range(1, len(layers)):
            section_number = min_index + rel_i + 1
            ip = render_flat_layer(layers[rel_i], bounds, scale, color_mode, background_color)
            stack.addSlice(str(section_number).zfill(4), ip)
        out = ImagePlus("Stack", stack)
        out.show()
        log("Done!")
        return

    pool = Executors.newFixedThreadPool(max_workers)
    try:
        for rel_i, layer in enumerate(layers):
            section_number = min_index + rel_i + 1
            if rel_i == 0:
                pool.submit(SectionExportTask(rel_i, section_number, layer, first_ip))
            else:
                pool.submit(SectionExportTask(rel_i, section_number, layer, None))
    finally:
        pool.shutdown()
        pool.awaitTermination(365, TimeUnit.DAYS)

    if CONFIG["errors"].size() > 0:
        raise RuntimeError("Export failed:\n" + "\n---\n".join([str(x) for x in list(CONFIG["errors"])]))

    log("Done!")


if __name__ == "__main__":
    main()