package org.openspaces.spatial;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.spatial.shapes.Shape;

import static org.openspaces.spatial.ShapeFactory.*;

public class ShapeFormatParseTest {

    @Test
    public void testWkt() {
        test(ShapeFormat.WKT);
    }

    @Test
    public void testGeoJson() {
        test(ShapeFormat.GEOJSON);
    }

    private void test(ShapeFormat shapeFormat) {
        Shape[] shapes = new Shape[] {
                point(1, 2),
                rectangle(1,2, 3,4),
                lineString(point(1,11), point(2,12), point(3,13)),
                polygon(point(0, 0), point(0, 5), point(5, 0)),
                polygon(point(0,0), point(0,5), point(2.5,2.5), point(5,5), point(5,0)),
                circle(point(0,0), 5)
        };

        for (Shape shape : shapes) {
            String s = shape.toString(shapeFormat);
            System.out.println("Shape in format " + shapeFormat + ": " + s);
            Shape parsed = ShapeFactory.parse(s, shapeFormat);
            Assert.assertEquals(shape, parsed);
        }
    }
}
