package eu.smartdatalake.simsearch.pivoting.rtree.geometry.internal;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Geometry;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Point;
import eu.smartdatalake.simsearch.pivoting.rtree.geometry.Rectangle;

public final class PointDouble implements Point {

    private final double[] x;

    private PointDouble(double[] x) {
        this.x = x;
    }

    public static PointDouble create(double x[]) {
        return new PointDouble(x);
    }

    @Override
    public Rectangle mbr() {
        return this;
    }

    @Override
    public double distance(Rectangle r) {
        return GeometryUtil.distance(x, r);
    }

    @Override
    public boolean intersects(Rectangle r) {
        return GeometryUtil.intersects(x, x, r.mins(), r.maxes());
    }

    @Override
    public double[] mins() {
        return x;
    }

    @Override
    public String toString() {
    	if (dimensions() > 1)
    		return "Point (" + Arrays.stream(mins()).mapToObj(String::valueOf).collect(Collectors.joining(" ")) + ")"; //Arrays.toString(mins());
    	else
    		return "" + mins()[0];
    }

    @Override
    public Geometry geometry() {
        return this;
    }

    @Override
    public double volume() {
        return 0;
    }

    @Override
    public Rectangle add(Rectangle r) {
        return Rectangle.create(GeometryUtil.min(x, r.mins()), GeometryUtil.max(x, r.maxes()));
    }

    @Override
    public boolean contains(double... p) {
        return Arrays.equals(x, p);
    }

    @Override
    public double intersectionVolume(Rectangle r) {
        return 0;
    }

    @Override
    public double surfaceArea() {
        return 0;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(x);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj.getClass() != getClass())
            return false;
        return Arrays.equals(x, ((PointDouble) obj).x);
    }

    @Override
    public int dimensions() {
        return x.length;
    }

}