package cliente.interfaceGrafica;

import javax.swing.*;
import java.awt.*;

/** Ponto colorido que indica o estado da conexão. */
public final class IconePonto implements Icon {

    private static final int DIAMETRO = 10;

    private Color cor;

    IconePonto (Color cor) { definirCor(cor); }

    void definirCor (Color nova) { cor = nova; }

    @Override public void paintIcon (Component componente, Graphics g, int x, int y) {

        Graphics2D g2 = (Graphics2D) g.create();

        try {
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

            g2.setColor(cor);

            g2.fillOval(x, y, DIAMETRO, DIAMETRO);
        }

        finally {

            g2.dispose();
        }
    }

    @Override public int getIconWidth () { return DIAMETRO; }

    @Override public int getIconHeight () { return DIAMETRO; }
}