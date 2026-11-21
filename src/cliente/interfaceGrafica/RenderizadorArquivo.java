package cliente.interfaceGrafica;

import javax.swing.*;
import java.awt.*;
import java.io.Serial;

import static cliente.interfaceGrafica.InterfaceGrafica.formatarTamanho;

/** Linha da lista: NOME à esquerda e TAMANHO à direita. */
public final class RenderizadorArquivo extends JPanel implements ListCellRenderer<ArquivoRemoto> {

    private final Color COR_TEXTO;

    private final Color COR_SELECAO = new Color(0xDBEAFE);

    private final Color COR_LINHA_ALTERNADA = new Color(0xF9FAFB);

    @Serial private static final long serialVersionUID = 1L;

    private final JLabel NOME = new JLabel();

    private final JLabel TAMANHO = new JLabel();



    RenderizadorArquivo (Color texto, Color foreground) {

        super(new BorderLayout(12, 0));

        COR_TEXTO = texto;

        setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 12));

        TAMANHO.setForeground(foreground);

        add(NOME, BorderLayout.CENTER);

        add(TAMANHO, BorderLayout.EAST);
    }



    @Override public Component getListCellRendererComponent (JList<? extends ArquivoRemoto> lista, ArquivoRemoto valor, int indice, boolean selecionado, boolean comFoco) {

        NOME.setText(valor.nome());

        TAMANHO.setText(formatarTamanho(valor.tamanho()));

        NOME.setForeground(COR_TEXTO);

        setBackground(selecionado? COR_SELECAO : indice % 2 == 0 ? Color.WHITE : COR_LINHA_ALTERNADA);

        return this;
    }
}