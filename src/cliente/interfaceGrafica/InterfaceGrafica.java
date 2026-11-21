package cliente.interfaceGrafica;

import cliente.*;
import cliente.componentes.Conexao;
import cliente.componentes.Direcao;
import cliente.componentes.EnderecoServidor;
import cliente.componentes.Nivel;
import cliente.componentes.ThreadTransferencia;
import cliente.fantoches.SocketTransferencia;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import java.io.File;
import java.io.IOException;
import java.io.Serial;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.KeyStroke;
import javax.swing.ListSelectionModel;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.ToolTipManager;
import javax.swing.UIManager;
import javax.swing.plaf.FontUIResource;
import javax.swing.text.BadLocationException;
import javax.swing.text.Element;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

/**
 * Interface gráfica (Swing) do cliente do Servidor de Arquivos via UDP.
 * Elementos da janela:
 * - Endereço e porta do servidor, com o estado da conexão;
 * - Lista de arquivos do servidor, mantida em sincronia com UPDATE_LIST;
 * - Botões de upload e download, com pasta de destino configurável;
 * - Barra de progresso e botão de cancelamento;
 * - Área de status com o histórico de mensagens.
 * Contrato com {@link Cliente}: o construtor não recebe argumentos, a janela se exibe
 * sozinha e os métodos {@link #atualizarLista(List)}, {@link #exibirMensagem(String)} e
 * {@link #exibirServidorIndisponivel(String)} podem ser chamados de qualquer thread.
 * Contrato com {@link ThreadTransferencia}: cada upload ou download cria uma
 * ThreadTransferencia com um {@link SocketTransferencia} próprio e um {@link Monitor} próprio.
 * Os métodos do Monitor chegam fora da EDT; eles apenas guardam valores, e um
 * {@link javax.swing.Timer} da EDT lê esses valores 10 vezes por segundo. Assim, a interface
 * continua fluida mesmo com milhares de fragmentos por segundo.
 * Regra de threads: todo acesso a componentes Swing acontece na EDT.
 * Rede, resolução de nomes (DNS) e transferências acontecem em threads separadas.
 * Endereço do servidor: {@link Cliente} não repassa endereço e porta à interface.
 * Por isso a interface lê esses valores da linha de comando (java cliente.Cliente [servidor] [porta]),
 * com os mesmos padrões de Cliente.main (localhost e 5000). Os campos da janela aceitam
 * correção manual, e {@link #definirServidor(String, int)} permite ao Cliente informar os valores exatos.
 */
public final class InterfaceGrafica extends JFrame {

    @Serial private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(InterfaceGrafica.class.getName());



//     constantes

    private static final String TITULO = "Servidor de Arquivos UDP";

    /** Mesmo padrão de Cliente.main quando a linha de comando não informa o servidor. */
    private static final String SERVIDOR_PADRAO = "localhost";

    /** Mesmo padrão de Cliente.main e de Servidor.PORTA. */
    private static final int PORTA_PADRAO = 5000;

    /** Nome da classe principal do cliente, usado para ler os argumentos da linha de comando. */
    private static final String CLASSE_PRINCIPAL = "cliente.Cliente";

    /** Intervalo de atualização da barra de progresso, em milissegundos. */
    private static final int INTERVALO_PROGRESSO_MS = 100;

    /** Quantidade máxima de linhas mantidas na área de status. */
    private static final int LIMITE_LINHAS_STATUS = 400;

    /** Maior arquivo que ThreadTransferencia consegue ler de uma vez (Files.readAllBytes). */
    private static final long TAMANHO_MAXIMO_UPLOAD = Integer.MAX_VALUE - 8L;

    private static final Locale PT_BR = Locale.forLanguageTag("pt-BR");

    private static final DateTimeFormatter FORMATO_HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Color COR_FUNDO = new Color(0xF3F4F6);

    /** Cor de preenchimento da barra de progresso (o Nimbus usa laranja por padrão). */
    private static final Color COR_PROGRESSO = new Color(0x2563EB);

    private static final Color COR_SUCESSO = new Color(0x15803D);

    private static final Color COR_ERRO = new Color(0xB91C1C);

    private static final Color COR_AVISO = new Color(0xB45309);

    private static final Color COR_BORDA = new Color(0xD1D5DB);

    private static final Color COR_TEXTO = new Color(0x1F2937);

    private static final Color COR_TEXTO_SUAVE = new Color(0x6B7280);

    static {

        configurarAparencia();
    }



//     componentes

    private final DefaultListModel<ArquivoRemoto> MODELO_ARQUIVOS = new DefaultListModel<>();

    private final JList<ArquivoRemoto> LISTA_ARQUIVOS = criarLista();

    private final JLabel ROTULO_CONTAGEM = new JLabel();

    private final transient IconePonto PONTO_CONEXAO = new IconePonto(COR_AVISO);

    private final JLabel ROTULO_CONEXAO = new JLabel("Conectando", PONTO_CONEXAO, JLabel.LEADING);

    private final JTextField CAMPO_SERVIDOR = new JTextField(14);

    private final JTextField CAMPO_PORTA = new JTextField(5);

    private final JButton BOTAO_ENVIAR = new JButton("Enviar arquivo…");

    private final JButton BOTAO_BAIXAR = new JButton("Baixar selecionado");

    private final JButton BOTAO_CANCELAR = new JButton("Cancelar");

    private final JButton BOTAO_PASTA = new JButton("Alterar…");

    private final JTextField CAMPO_PASTA = new JTextField();

    private final JProgressBar BARRA_PROGRESSO = new JProgressBar(0, 100);

    private final JLabel ROTULO_STATUS = new JLabel(" ");

    private final JTextPane AREA_STATUS = new JTextPane();

    private final Timer TEMPORIZADOR_PROGRESSO = new Timer(INTERVALO_PROGRESSO_MS, e -> aplicarProgresso());



//     estado (acessado somente na EDT)

    private Conexao conexao = Conexao.CONECTANDO;

    private transient MonitorDaTransferencia transferencia;

    private transient Path pastaDestino = pastaPadraoDeDownload();

    private File pastaUltimoUpload;

    private transient List<ArquivoRemoto> arquivosAtuais = Collections.emptyList();



//     construção

    /**
     * Cria a janela e a exibe. Deve ser chamado na EDT, como faz Cliente.main.
     */
    public InterfaceGrafica () {

        this(true);
    }



    /**
     * Cria a janela e a exibe somente quando {@code exibir} for verdadeiro.
     * Útil para testes automatizados no mesmo pacote.
     */
    private InterfaceGrafica (boolean exibir) {

        super(TITULO);

        prepararComponentes();

        setContentPane(montarConteudo());

        prepararJanela();

        atualizarContagem();

        definirStatus("Conectando ao servidor…", COR_TEXTO_SUAVE);

        atualizarControles();

        if (exibir) {

            setVisible(true);

            LISTA_ARQUIVOS.requestFocusInWindow();
        }
    }



//     API usada por Cliente (qualquer thread)

    /**
     * Atualiza a lista de arquivos exibida. Chamado por Cliente a cada LIST ou UPDATE_LIST.
     * A primeira lista recebida confirma a conexão e habilita os botões.
     * Aceita itens no formato "nome" ou "nome;tamanho" (ver comum.Datagrama). Quando Cliente divide a
     * lista por ponto-e-vírgula, nomes e tamanhos chegam alternados, e a interface os reagrupa.
     *
     * @param lista nomes dos arquivos do servidor; null equivale a lista vazia
     */
    public void atualizarLista (List<String> lista) {

        final List<String> copia = lista == null? Collections.emptyList() : new ArrayList<>(lista);

        executarNaEdt(() -> aplicarLista(interpretarLista(copia)));
    }



    /**
     * Exibe uma mensagem informativa na área de status.
     *
     * @param mensagem texto a exibir; texto vazio é ignorado
     */
    public void exibirMensagem (String mensagem) {

        if (mensagem == null || mensagem.isBlank()) return;

        executarNaEdt(() -> {

            registrar(Nivel.INFO, mensagem);

            if (transferencia == null) definirStatus(mensagem, COR_TEXTO_SUAVE);
        });
    }



    /**
     * Informa que o servidor está indisponível: desabilita as operações, destaca o erro
     * na janela e exibe uma caixa de diálogo.
     *
     * @param mensagem motivo da indisponibilidade
     */
    public void exibirServidorIndisponivel (String mensagem) {

        final String texto = mensagem == null || mensagem.isBlank() ? "O servidor deixou de responder." : mensagem;

        executarNaEdt(() -> {

            definirConexao(Conexao.INDISPONIVEL);

            registrar(Nivel.ERRO, texto);

            if (transferencia == null) definirStatus(texto, COR_ERRO);

            atualizarControles();

            mostrarErro("Servidor indisponível", texto);
        });
    }



    /**
     * Informa o endereço exato do servidor. Uso opcional: sem esta chamada, a interface usa os
     * argumentos da linha de comando.
     *
     * @param host  endereço IP ou nome do servidor
     * @param porta porta UDP do servidor
     */
    public void definirServidor (String host, int porta) {

        executarNaEdt(() -> {

            CAMPO_SERVIDOR.setText(host == null ? "" : host.trim());

            CAMPO_PORTA.setText(String.valueOf(porta));
        });
    }



//     montagem da janela

    private void prepararComponentes () {

        EnderecoServidor servidor = inferirServidor(System.getProperty("sun.java.command"));

        CAMPO_SERVIDOR.setText(servidor.host());

        CAMPO_PORTA.setText(String.valueOf(servidor.porta()));

        CAMPO_SERVIDOR.setToolTipText("Endereço IP ou nome do servidor");

        CAMPO_PORTA.setToolTipText("Porta UDP do servidor");

        CAMPO_PASTA.setEditable(false);

        CAMPO_PASTA.setToolTipText("Pasta onde os arquivos baixados são salvos");

        definirPastaDestino(pastaDestino);

        BOTAO_ENVIAR.setMnemonic(KeyEvent.VK_E);

        BOTAO_ENVIAR.setToolTipText("Escolher um arquivo do computador e enviá-lo ao servidor");

        BOTAO_BAIXAR.setMnemonic(KeyEvent.VK_B);

        BOTAO_BAIXAR.setToolTipText("Baixar o arquivo selecionado para a pasta de destino");

        BOTAO_CANCELAR.setMnemonic(KeyEvent.VK_C);

        BOTAO_CANCELAR.setToolTipText("Cancelar a transferência em andamento");

        BOTAO_PASTA.setMnemonic(KeyEvent.VK_A);

        BOTAO_PASTA.setToolTipText("Escolher outra pasta de destino");

        BARRA_PROGRESSO.setStringPainted(false);

        ROTULO_CONTAGEM.setForeground(COR_TEXTO_SUAVE);

        ROTULO_CONEXAO.setFont(ROTULO_CONEXAO.getFont().deriveFont(Font.BOLD));

        ROTULO_CONEXAO.setIconTextGap(6);

        AREA_STATUS.setEditable(false);

        AREA_STATUS.setBackground(Color.WHITE);

        AREA_STATUS.setBorder(BorderFactory.createEmptyBorder(6, 8, 6, 8));

        Style base = AREA_STATUS.addStyle("hora", null);

        StyleConstants.setForeground(base, COR_TEXTO_SUAVE);

        StyleConstants.setFontFamily(base, Font.MONOSPACED);

        estiloDoNivel(Nivel.INFO, COR_TEXTO);

        estiloDoNivel(Nivel.SUCESSO, COR_SUCESSO);

        estiloDoNivel(Nivel.ERRO, COR_ERRO);

        estiloDoNivel(Nivel.AVISO, COR_AVISO);

        nomear();

        registrarEventos();
    }



    private void estiloDoNivel (Nivel nivel, Color cor) {

        Style estilo = AREA_STATUS.addStyle(nivel.name(), null);

        StyleConstants.setForeground(estilo, cor);
    }



    /** Nomes dos componentes: facilitam testes automatizados e tecnologias assistivas. */
    private void nomear () {

        LISTA_ARQUIVOS.setName("listaArquivos");

        CAMPO_SERVIDOR.setName("campoServidor");

        CAMPO_PORTA.setName("campoPorta");

        CAMPO_PASTA.setName("campoPasta");

        BOTAO_ENVIAR.setName("botaoEnviar");

        BOTAO_BAIXAR.setName("botaoBaixar");

        BOTAO_CANCELAR.setName("botaoCancelar");

        BOTAO_PASTA.setName("botaoPasta");

        BARRA_PROGRESSO.setName("barraProgresso");

        ROTULO_STATUS.setName("rotuloStatus");

        ROTULO_CONEXAO.setName("rotuloConexao");

        ROTULO_CONTAGEM.setName("rotuloContagem");

        AREA_STATUS.setName("areaStatus");

        LISTA_ARQUIVOS.getAccessibleContext().setAccessibleName("Arquivos no servidor");

        AREA_STATUS.getAccessibleContext().setAccessibleName("Histórico de mensagens");
    }



    private void registrarEventos () {

        BOTAO_ENVIAR.addActionListener(e -> escolherArquivoEEnviar());

        BOTAO_BAIXAR.addActionListener(e -> baixarSelecionado());

        BOTAO_CANCELAR.addActionListener(e -> cancelarTransferencia());

        BOTAO_PASTA.addActionListener(e -> escolherPastaDestino());

        LISTA_ARQUIVOS.addListSelectionListener(e -> {

            if (!e.getValueIsAdjusting()) atualizarControles();
        });

        LISTA_ARQUIVOS.addMouseListener(new MouseAdapter() {

            @Override public void mouseClicked (MouseEvent e) {

                if (e.getClickCount() != 2 || e.getButton() != MouseEvent.BUTTON1) return;

                int indice = LISTA_ARQUIVOS.locationToIndex(e.getPoint());

                if (indice < 0) return;

                Rectangle area = LISTA_ARQUIVOS.getCellBounds(indice, indice);

                if (area != null && area.contains(e.getPoint())) baixarSelecionado();
            }
        });

        LISTA_ARQUIVOS.getInputMap(JComponent.WHEN_FOCUSED).put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "baixar");

        LISTA_ARQUIVOS.getActionMap().put("baixar", new AbstractAction() {

            @Serial private static final long serialVersionUID = 1L;

            @Override public void actionPerformed (ActionEvent e) { baixarSelecionado(); }
        });

        addWindowListener(new WindowAdapter() {

            @Override public void windowClosing (WindowEvent e) { encerrarAplicacao(); }
        });
    }



    private JComponent montarConteudo () {

        JPanel raiz = new JPanel(new BorderLayout(0, 14));

        raiz.setBackground(COR_FUNDO);

        raiz.setBorder(BorderFactory.createEmptyBorder(16, 18, 16, 18));

        raiz.add(montarBarraDoServidor(), BorderLayout.NORTH);

        raiz.add(montarAreaDaLista(), BorderLayout.CENTER);

        raiz.add(montarRodape(), BorderLayout.SOUTH);

        return raiz;
    }



    private JComponent montarBarraDoServidor () {

        JPanel barra = new JPanel(new BorderLayout(12, 0));

        barra.setOpaque(false);

        JPanel campos = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));

        campos.setOpaque(false);

        campos.add(rotuloEmNegrito("Servidor"));

        campos.add(CAMPO_SERVIDOR);

        campos.add(new JLabel(":"));

        campos.add(CAMPO_PORTA);

        barra.add(campos, BorderLayout.WEST);

        barra.add(ROTULO_CONEXAO, BorderLayout.EAST);

        return barra;
    }



    private JComponent montarAreaDaLista () {

        JPanel area = new JPanel(new BorderLayout(0, 6));

        area.setOpaque(false);

        JPanel cabecalho = new JPanel(new BorderLayout());

        cabecalho.setOpaque(false);

        cabecalho.add(rotuloEmNegrito("Arquivos no servidor"), BorderLayout.WEST);

        cabecalho.add(ROTULO_CONTAGEM, BorderLayout.EAST);

        JScrollPane rolagem = new JScrollPane(LISTA_ARQUIVOS);

        rolagem.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);

        rolagem.setBorder(BorderFactory.createLineBorder(COR_BORDA));

        rolagem.getViewport().setBackground(Color.WHITE);

        area.add(cabecalho, BorderLayout.NORTH);

        area.add(rolagem, BorderLayout.CENTER);

        return area;
    }



    private JComponent montarRodape () {

        JPanel rodape = new JPanel(new GridBagLayout());

        rodape.setOpaque(false);

//      Linha 0: ações de envio e de download, e pasta de destino

        JPanel acoes = new JPanel(new GridBagLayout());

        acoes.setOpaque(false);

        acoes.add(BOTAO_ENVIAR, celula(0, 0, 0, GridBagConstraints.NONE, new Insets(0, 0, 0, 8)));

        acoes.add(BOTAO_BAIXAR, celula(1, 0, 0, GridBagConstraints.NONE, new Insets(0, 0, 0, 18)));

        acoes.add(new JLabel("Salvar em"), celula(2, 0, 0, GridBagConstraints.NONE, new Insets(0, 0, 0, 8)));

        acoes.add(CAMPO_PASTA, celula(3, 0, 1, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 8)));

        acoes.add(BOTAO_PASTA, celula(4, 0, 0, GridBagConstraints.NONE, new Insets(0, 0, 0, 0)));

        rodape.add(acoes, celula(0, 0, 1, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 12, 0)));

//      Linha 1: barra de progresso e cancelamento

        JPanel progresso = new JPanel(new BorderLayout(10, 0));

        progresso.setOpaque(false);

        progresso.add(BARRA_PROGRESSO, BorderLayout.CENTER);

        progresso.add(BOTAO_CANCELAR, BorderLayout.EAST);

        rodape.add(progresso, celula(0, 1, 1, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 6, 0)));

//      Linha 2: status ao vivo

        rodape.add(ROTULO_STATUS, celula(0, 2, 1, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 10, 0)));

//      Linha 3: histórico

        JScrollPane historico = new JScrollPane(AREA_STATUS);

        historico.setBorder(BorderFactory.createLineBorder(COR_BORDA));

        historico.setPreferredSize(new Dimension(100, 130));

        rodape.add(historico, celula(0, 3, 1, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0)));

        return rodape;
    }



    private static GridBagConstraints celula (int x, int y, double pesoX, int preenchimento, Insets margens) {

        GridBagConstraints c = new GridBagConstraints();

        c.gridx = x;

        c.gridy = y;

        c.weightx = pesoX;

        c.fill = preenchimento;

        c.insets = margens;

        c.anchor = GridBagConstraints.WEST;

        return c;
    }



    private static JLabel rotuloEmNegrito (String texto) {

        JLabel rotulo = new JLabel(texto);

        rotulo.setFont(rotulo.getFont().deriveFont(Font.BOLD));

        return rotulo;
    }



    private void prepararJanela () {

        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

        setMinimumSize(new Dimension(680, 540));

        setSize(820, 660);

        setLocationRelativeTo(null);
    }



    /**
     * Lista de arquivos com linhas alternadas, tamanho à direita, dica de uso e mensagem
     * de lista vazia.
     */
    private JList<ArquivoRemoto> criarLista () {

        JList<ArquivoRemoto> lista = new JList<>(MODELO_ARQUIVOS) {

            @Serial private static final long serialVersionUID = 1L;

            @Override protected void paintComponent (Graphics g) {

                super.paintComponent(g);

                if (MODELO_ARQUIVOS.isEmpty()) desenharMensagemDaLista(g, this);
            }

            @Override public boolean getScrollableTracksViewportWidth () { return true; }

            @Override public String getToolTipText(MouseEvent evento) {

                int indice = locationToIndex(evento.getPoint());

                if (indice < 0) return null;

                Rectangle area = getCellBounds(indice, indice);

                if (area == null || !area.contains(evento.getPoint())) return null;

                ArquivoRemoto arquivo = MODELO_ARQUIVOS.getElementAt(indice);

                String tamanho = formatarTamanho(arquivo.tamanho());

                return tamanho.isEmpty() ? arquivo.nome() : arquivo.nome() + " — " + tamanho;
            }
        };

        lista.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        lista.setCellRenderer(new RenderizadorArquivo(COR_TEXTO, COR_TEXTO_SUAVE));

        lista.setFixedCellHeight(32);

        ToolTipManager.sharedInstance().registerComponent(lista);

        return lista;
    }



    private void desenharMensagemDaLista (Graphics g, JComponent componente) {

        Graphics2D g2 = (Graphics2D) g.create();

        try {
            g2.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);

            g2.setColor(COR_TEXTO_SUAVE);

            String texto = conexao == Conexao.CONECTANDO ? "Conectando ao servidor…"
                         : conexao == Conexao.INDISPONIVEL ? "Servidor indisponível"
                         : "Lista vazia. Envie o primeiro arquivo.";

            FontMetrics medidas = g2.getFontMetrics();

            int x = (componente.getWidth() - medidas.stringWidth(texto)) / 2;

            int y = componente.getHeight() / 2 + medidas.getAscent() / 2;

            g2.drawString(texto, Math.max(x, 8), y);
        }

        finally {

            g2.dispose();
        }
    }



//     lista de arquivos

    private void aplicarLista (List<ArquivoRemoto> novos) {

        boolean primeiraLista = conexao != Conexao.CONECTADO;

        definirConexao(Conexao.CONECTADO);

        boolean mudou = !novos.equals(arquivosAtuais);

        if (mudou) {

            ArquivoRemoto selecionado = LISTA_ARQUIVOS.getSelectedValue();

            String nomeSelecionado = selecionado == null ? null : selecionado.nome();

            arquivosAtuais = novos;

            MODELO_ARQUIVOS.clear();

            MODELO_ARQUIVOS.addAll(novos);

            restaurarSelecao(nomeSelecionado);
        }

        atualizarContagem();

        if (primeiraLista) {

            registrar(Nivel.SUCESSO, "Conectado ao servidor. " + descreverQuantidade(novos.size()) + ".");

            if (transferencia == null) definirStatus("Pronto.", COR_TEXTO_SUAVE);
        }

        else if (mudou) registrar(Nivel.INFO, "Lista atualizada: " + descreverQuantidade(novos.size()) + ".");

        atualizarControles();

        LISTA_ARQUIVOS.repaint();
    }



    private void restaurarSelecao (String nome) {

        if (nome == null) return;

        for (int i = 0; i < MODELO_ARQUIVOS.size(); i++) {

            if (MODELO_ARQUIVOS.get(i).nome().equals(nome)) {

                LISTA_ARQUIVOS.setSelectedIndex(i);

                return;
            }
        }
    }



    private void atualizarContagem () {

        ROTULO_CONTAGEM.setText(descreverQuantidade(MODELO_ARQUIVOS.size()));
    }



    private static String descreverQuantidade (int quantidade) {

        return quantidade == 1? "1 arquivo" : quantidade + " arquivos";
    }



    /**
     * Converte os itens recebidos de Cliente em arquivos do servidor.
     * Formatos aceitos:
     * - "nome" (apenas o nome);
     * - "nome;tamanho" (formato de comum.Datagrama), em um item ou em várias linhas;
     * - nomes e tamanhos alternados, resultado de dividir "nome;tamanho" por ponto-e-vírgula:
     *   quando a quantidade é par, as posições pares não são numéricas e as ímpares são numéricas,
     *   os pares são reagrupados.
     * A lista devolvida está em ordem alfabética, sem diferenciar maiúsculas e minúsculas.
     */
    static List<ArquivoRemoto> interpretarLista (List<String> bruta) {

        List<String> itens = new ArrayList<>();

        if (bruta != null) {

            for (String elemento : bruta) {

                if (elemento == null) continue;

                for (String linha : elemento.split("\\r?\\n")) {

                    if (!linha.isBlank()) itens.add(linha.trim());
                }
            }
        }

        List<ArquivoRemoto> arquivos = new ArrayList<>();

        if (ehListaAlternada(itens)) {

            for (int i = 0; i < itens.size(); i += 2) arquivos.add(new ArquivoRemoto(itens.get(i), lerTamanho(itens.get(i + 1))));
        }

        else {

            for (String item : itens) arquivos.add(interpretarItem(item));
        }

        arquivos.sort(Comparator.comparing(ArquivoRemoto::nome, String.CASE_INSENSITIVE_ORDER));

        return arquivos;
    }



    private static boolean ehListaAlternada (List<String> itens) {

        if (itens.size() < 2 || itens.size() % 2 != 0) return false;

        for (int i = 0; i < itens.size(); i++) {

            String item = itens.get(i);

            if (item.indexOf(';') >= 0) return false;

            boolean numerico = ehInteiroNaoNegativo(item);

            boolean posicaoDeTamanho = i % 2 == 1;

            if (numerico != posicaoDeTamanho) return false;
        }

        return true;
    }



    private static ArquivoRemoto interpretarItem (String item) {

        int separador = item.lastIndexOf(';');

        if (separador > 0 && ehInteiroNaoNegativo(item.substring(separador + 1).trim())) {

            return new ArquivoRemoto(item.substring(0, separador).trim(), lerTamanho(item.substring(separador + 1).trim()));
        }

        return new ArquivoRemoto(item, -1);
    }



    private static boolean ehInteiroNaoNegativo (String texto) {

        if (texto.isEmpty() || texto.length() > 18) return false;

        for (int i = 0; i < texto.length(); i++) {

            if (texto.charAt(i) < '0' || texto.charAt(i) > '9') return false;
        }

        return true;
    }



    private static long lerTamanho (String texto) {

        return ehInteiroNaoNegativo(texto) ? Long.parseLong(texto) : -1;
    }



    /** Formata bytes em B, KB, MB, GB ou TB, com vírgula decimal. Devolve texto vazio para tamanho desconhecido. */
    static String formatarTamanho (long bytes) {

        if (bytes < 0) return "";

        if (bytes < 1024) return bytes + " B";

        String[] unidades = { "KB", "MB", "GB", "TB" };

        double valor = bytes;

        int indice = -1;

        do {
            valor /= 1024;

            indice++;
        }

        while (valor >= 1024 && indice < unidades.length - 1);

        return String.format(PT_BR, "%.1f %s", valor, unidades[indice]);
    }



    /**
     * Lê servidor e porta da linha de comando da JVM (propriedade sun.java.command), no formato
     * "cliente.Cliente [servidor] [porta]". Valores ausentes ou inválidos usam os padrões de Cliente.main.
     */
    static EnderecoServidor inferirServidor (String linhaDeComando) {

        String host = SERVIDOR_PADRAO;

        int porta = PORTA_PADRAO;

        if (linhaDeComando != null) {

            String[] partes = linhaDeComando.trim().split("\\s+");

            for (int i = 0; i < partes.length; i++) {

                if (!partes[i].equals(CLASSE_PRINCIPAL)) continue;

                if (i + 1 < partes.length) host = partes[i + 1];

                if (i + 2 < partes.length && ehInteiroNaoNegativo(partes[i + 2])) {

                    int candidata = Integer.parseInt(partes[i + 2]);

                    if (candidata >= 1 && candidata <= 65535) porta = candidata;
                }

                break;
            }
        }

        return new EnderecoServidor(host, porta);
    }



//     conexão e controles

    private void definirConexao (Conexao nova) {

        conexao = nova;

        if (nova == Conexao.CONECTADO) {

            PONTO_CONEXAO.definirCor(COR_SUCESSO);

            ROTULO_CONEXAO.setText("Conectado");
        }

        else if (nova == Conexao.INDISPONIVEL) {

            PONTO_CONEXAO.definirCor(COR_ERRO);

            ROTULO_CONEXAO.setText("Indisponível");
        }

        else {

            PONTO_CONEXAO.definirCor(COR_AVISO);

            ROTULO_CONEXAO.setText("Conectando");
        }

        ROTULO_CONEXAO.repaint();

        LISTA_ARQUIVOS.repaint();
    }



    private void atualizarControles () {

        boolean conectado = conexao == Conexao.CONECTADO;

        boolean ocupado = transferencia != null;

        boolean selecionado = LISTA_ARQUIVOS.getSelectedValue() != null;

        BOTAO_ENVIAR.setEnabled(conectado && !ocupado);

        BOTAO_BAIXAR.setEnabled(conectado && !ocupado && selecionado);

        BOTAO_CANCELAR.setEnabled(ocupado);

        BOTAO_PASTA.setEnabled(!ocupado);

        CAMPO_SERVIDOR.setEnabled(!ocupado);

        CAMPO_PORTA.setEnabled(!ocupado);
    }



    private void definirStatus (String texto, Color cor) {

        ROTULO_STATUS.setForeground(cor);

        ROTULO_STATUS.setText(texto == null || texto.isEmpty() ? " " : texto);

        ROTULO_STATUS.setToolTipText(texto == null || texto.isBlank() ? null : texto);
    }



    private void registrar (Nivel nivel, String texto) {

        StyledDocument documento = AREA_STATUS.getStyledDocument();

        try {
            documento.insertString(documento.getLength(), FORMATO_HORA.format(LocalTime.now()) + "  ", AREA_STATUS.getStyle("hora"));

            documento.insertString(documento.getLength(), texto + "\n", AREA_STATUS.getStyle(nivel.name()));

            Element raiz = documento.getDefaultRootElement();

            while (raiz.getElementCount() > LIMITE_LINHAS_STATUS) {

                documento.remove(0, raiz.getElement(0).getEndOffset());
            }
        }

        catch (BadLocationException e) {

            LOGGER.log(Level.FINE, "Posição inválida na área de status", e);
        }

        AREA_STATUS.setCaretPosition(documento.getLength());
    }



//     upload

    private void escolherArquivoEEnviar () {

        JFileChooser seletor = new JFileChooser(pastaUltimoUpload);

        seletor.setDialogTitle("Escolha o arquivo para enviar ao servidor");

        seletor.setFileSelectionMode(JFileChooser.FILES_ONLY);

        if (seletor.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;

        File escolhido = seletor.getSelectedFile();

        pastaUltimoUpload = escolhido.getParentFile();

        iniciarUpload(escolhido.toPath());
    }



    /**
     * Valida o arquivo e inicia o envio. Deve ser chamado na EDT.
     */
    void iniciarUpload (Path arquivo) {

        if (transferencia != null || arquivo == null) return;

        long tamanho;

        try {
            if (!Files.isRegularFile(arquivo) || !Files.isReadable(arquivo)) {

                mostrarAviso("Selecione um arquivo comum e legível.");

                return;
            }

            tamanho = Files.size(arquivo);
        }

        catch (IOException e) {

            mostrarAviso("Falha ao ler o arquivo: " + e.getMessage());

            return;
        }

        if (tamanho == 0) {

            mostrarAviso("O arquivo está vazio. Selecione um arquivo com conteúdo.");

            return;
        }

        if (tamanho > TAMANHO_MAXIMO_UPLOAD) {

            mostrarAviso("O arquivo tem " + formatarTamanho(tamanho) + " e ultrapassa o limite de 2 GB por envio.");

            return;
        }

        String nome = arquivo.getFileName().toString();

        iniciarTransferencia(Direcao.UPLOAD, nome, arquivo, "Enviando \"" + nome + "\" (" + formatarTamanho(tamanho) + ")");
    }



//     download

    private void baixarSelecionado () {

        ArquivoRemoto selecionado = LISTA_ARQUIVOS.getSelectedValue();

        if (selecionado == null || !BOTAO_BAIXAR.isEnabled()) return;

        iniciarDownload(selecionado.nome(), pastaDestino);
    }



    private void escolherPastaDestino () {

        JFileChooser seletor = new JFileChooser(pastaDestino.toFile());

        seletor.setDialogTitle("Escolha a pasta de destino dos downloads");

        seletor.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        if (seletor.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) definirPastaDestino(seletor.getSelectedFile().toPath());
    }



    private void definirPastaDestino (Path pasta) {

        pastaDestino = pasta;

        CAMPO_PASTA.setText(pasta.toString());

        CAMPO_PASTA.setCaretPosition(0);

        CAMPO_PASTA.setToolTipText(pasta.toString());
    }



    /**
     * Valida o destino e inicia o download. Deve ser chamado na EDT.
     */
    void iniciarDownload (String nome, Path pasta) {

        if (transferencia != null || nome == null || pasta == null) return;

        if (nome.isBlank() || nome.contains("/") || nome.contains("\\") || nome.equals(".") || nome.equals("..")) {

            mostrarAviso("O nome \"" + nome + "\" contém separadores de pasta. O download aceita apenas nomes simples de arquivo.");

            return;
        }

        if (!Files.isDirectory(pasta) || !Files.isWritable(pasta)) {

            mostrarAviso("Escolha uma pasta de destino existente e com permissão de escrita.");

            return;
        }

        Path destino;

        try {
            destino = pasta.resolve(nome);
        }

        catch (InvalidPathException e) {

            mostrarAviso("O nome \"" + nome + "\" é inválido neste sistema de arquivos.");

            return;
        }

        if (Files.exists(destino)) {

            int resposta = JOptionPane.showConfirmDialog(this,
                    "O arquivo \"" + nome + "\" já existe em:\n" + pasta + "\nDeseja substituí-lo?",
                    "Substituir arquivo", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);

            if (resposta != JOptionPane.YES_OPTION) return;
        }

        iniciarTransferencia(Direcao.DOWNLOAD, nome, pasta, "Baixando \"" + nome + "\" para " + pasta);
    }



//     transferências

    /**
     * Inicia upload ou download em segundo plano.
     *
     * @param direcao      UPLOAD ou DOWNLOAD
     * @param nome         nome do arquivo no servidor
     * @param caminhoLocal arquivo de origem (upload) ou pasta de destino (download)
     * @param descricao    texto registrado na área de status
     */
    private void iniciarTransferencia (Direcao direcao, String nome, Path caminhoLocal, String descricao) {

        final String host = CAMPO_SERVIDOR.getText().trim();

        if (host.isEmpty()) {

            mostrarAviso("Informe o endereço do servidor.");

            CAMPO_SERVIDOR.requestFocusInWindow();

            return;
        }

        final int porta = lerPorta();

        if (porta < 0) {

            mostrarAviso("Informe uma porta entre 1 e 65535.");

            CAMPO_PORTA.requestFocusInWindow();

            return;
        }

        MonitorDaTransferencia monitor = new MonitorDaTransferencia(this::finalizarTransferencia);

        transferencia = monitor;

        BARRA_PROGRESSO.setValue(0);

        BARRA_PROGRESSO.setIndeterminate(true);

        BARRA_PROGRESSO.setStringPainted(true);

        BARRA_PROGRESSO.setString("Preparando…");

        definirStatus("Preparando a transferência…", COR_TEXTO_SUAVE);

        registrar(Nivel.INFO, descricao + " — servidor " + host + ":" + porta + ".");

        TEMPORIZADOR_PROGRESSO.start();

        atualizarControles();

        Thread preparo = new Thread(() -> prepararTransferencia(monitor, direcao, host, porta, nome, caminhoLocal), "interface-preparo-transferencia");

        preparo.setDaemon(true);

        preparo.start();
    }



    private int lerPorta () {

        try {
            int porta = Integer.parseInt(CAMPO_PORTA.getText().trim());

            return porta >= 1 && porta <= 65535 ? porta : -1;
        }

        catch (NumberFormatException e) {

            return -1;
        }
    }



    /**
     * Resolve o endereço, abre os sockets e dispara a ThreadTransferencia. Roda fora da EDT,
     * porque a resolução de nomes pode demorar.
     */
    private void prepararTransferencia (MonitorDaTransferencia monitor, Direcao direcao, String host, int porta, String nome, Path caminhoLocal) {

        try {
            InetSocketAddress destino = new InetSocketAddress(InetAddress.getByName(host), porta);

            SocketTransferencia meia = new SocketTransferencia();

            monitor.adicionarRecurso(meia::fechar);

            ThreadTransferencia thread = ThreadTransferencia.criar(direcao, destino, nome, caminhoLocal, monitor, meia);

            thread.setDaemon(true);

            thread.setUncaughtExceptionHandler((t, erro) -> {

                LOGGER.log(Level.SEVERE, "Falha inesperada na transferência", erro);

                monitor.onErro(erro instanceof OutOfMemoryError? "Memória insuficiente para processar este arquivo." : "Falha inesperada: " + erro);
            });

            if (!monitor.vincular(thread)) return;

            thread.start();

            iniciarVigia(thread, monitor);
        }

        catch (UnknownHostException e) {

            monitor.onErro("Endereço do servidor inválido: " + host);
        }

        catch (IOException | RuntimeException e) {

            LOGGER.log(Level.WARNING, "Falha ao preparar a transferência", e);

            monitor.onErro("Falha ao preparar a transferência: " + e.getMessage());
        }
    }



    /**
     * Garante que a interface seja liberada mesmo se a ThreadTransferencia terminar sem
     * chamar onConcluido ou onErro.
     */
    private static void iniciarVigia (Thread thread, MonitorDaTransferencia monitor) {

        Thread vigia = new Thread(() -> {

            try {
                thread.join();
            }

            catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                return;
            }

            monitor.onErro("A transferência terminou antes de informar o resultado.");
        }, "interface-vigia-transferencia");

        vigia.setDaemon(true);

        vigia.start();
    }



    private void cancelarTransferencia () {

        if (transferencia != null) transferencia.cancelar();
    }



    /** Atualiza barra e status ao vivo com os valores mais recentes enviados pela ThreadTransferencia. */
    private void aplicarProgresso () {

        MonitorDaTransferencia atual = transferencia;

        if (atual == null || atual.encerrada()) return;

        double fracao = atual.fracao;

        if (fracao < 0) {

            BARRA_PROGRESSO.setIndeterminate(true);

            BARRA_PROGRESSO.setString("Preparando…");
        }

        else {

            int percentual = (int) Math.round(Math.max(0.0, Math.min(1.0, fracao)) * 100);

            BARRA_PROGRESSO.setIndeterminate(false);

            BARRA_PROGRESSO.setValue(percentual);

            BARRA_PROGRESSO.setString(percentual + "%");
        }

        String status = atual.status;

        if (!status.isEmpty()) definirStatus(status, COR_TEXTO_SUAVE);
    }



    /** Conclui a transferência na EDT. Resultados de transferências antigas são ignorados. */
    private void finalizarTransferencia (MonitorDaTransferencia monitor, boolean sucesso, String mensagem) {

        if (monitor != transferencia) return;

        TEMPORIZADOR_PROGRESSO.stop();

        transferencia = null;

        BARRA_PROGRESSO.setIndeterminate(false);

        if (sucesso) {

            BARRA_PROGRESSO.setValue(100);

            BARRA_PROGRESSO.setStringPainted(true);

            BARRA_PROGRESSO.setString("100%");

            registrar(Nivel.SUCESSO, mensagem);

            definirStatus(mensagem, COR_SUCESSO);
        }

        else {

            BARRA_PROGRESSO.setValue(0);

            BARRA_PROGRESSO.setStringPainted(false);

            if (monitor.cancelada) {

                registrar(Nivel.AVISO, mensagem);

                definirStatus(mensagem, COR_AVISO);
            }

            else {

                registrar(Nivel.ERRO, mensagem);

                definirStatus(mensagem, COR_ERRO);
            }
        }

        atualizarControles();

        if (!sucesso && !monitor.cancelada) mostrarErro("Falha na transferência", mensagem);
    }



    private void encerrarAplicacao () {

        if (transferencia != null) {

            int resposta = JOptionPane.showConfirmDialog(this,
                    "Há uma transferência em andamento.\nDeseja cancelá-la e sair?",
                    "Sair", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);

            if (resposta != JOptionPane.YES_OPTION) return;

            cancelarTransferencia();
        }

        dispose();

        System.exit(0);
    }



    private void mostrarAviso (String mensagem) {

        JOptionPane.showMessageDialog(this, mensagem, "Atenção", JOptionPane.WARNING_MESSAGE);
    }



    private void mostrarErro (String titulo, String mensagem) {

        JOptionPane.showMessageDialog(this, mensagem, titulo, JOptionPane.ERROR_MESSAGE);
    }



    private static void executarNaEdt (Runnable tarefa) {

        if (SwingUtilities.isEventDispatchThread()) tarefa.run();

        else SwingUtilities.invokeLater(tarefa);
    }



    private static Path pastaPadraoDeDownload () {

        try {
            Path casa = Paths.get(System.getProperty("user.home", "."));

            Path downloads = casa.resolve("Downloads");

            return Files.isDirectory(downloads) ? downloads : casa;
        }

        catch (InvalidPathException e) {

            return Paths.get("").toAbsolutePath();
        }
    }



    /** Aparência Nimbus (incluída no JDK), igual em todos os sistemas. Em caso de falha, vale a aparência padrão. */
    private static void configurarAparencia () {

        try {
            UIManager.put("control", COR_FUNDO);

            UIManager.put("nimbusOrange", COR_PROGRESSO);

            UIManager.put("defaultFont", new FontUIResource(Font.SANS_SERIF, Font.PLAIN, 13));

            for (UIManager.LookAndFeelInfo aparencia : UIManager.getInstalledLookAndFeels()) {

                if ("Nimbus".equals(aparencia.getName())) {

                    UIManager.setLookAndFeel(aparencia.getClassName());

                    return;
                }
            }
        }

        catch (Exception | LinkageError e) {

            LOGGER.log(Level.FINE, "Aparência Nimbus indisponível; mantendo a aparência padrão", e);
        }
    }
}