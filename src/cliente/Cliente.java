package cliente;

import cliente.UDP.ProtocoloUDP;
import cliente.interfaceGrafica.InterfaceGrafica;
import comum.Comando;
import comum.Datagrama;
import javax.swing.SwingUtilities;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Classe principal do cliente do Servidor de Arquivos via UDP.
 * Responsabilidades:
 * - Abrir a interface gráfica (InterfaceGrafica).
 * - Identificar o cliente com um id único.
 * - Registrar o cliente no servidor com o comando REGISTER, em thread separada da EDT.
 * - Iniciar uma thread exclusiva para escutar UPDATE_LIST.
 * - Detectar servidor indisponível por timeout no REGISTER ou no LIST.
 * - Entregar as listas atualizadas à interface na thread correta (SwingUtilities.invokeLater).
 */
public class Cliente {

    /** Tempo de espera máximo para aguardar a resposta do REGISTER, em milissegundos. */
    private static final int TIMEOUT_REGISTRO_MS = 2000;

    /** Tempo de espera máximo para aguardar a primeira resposta do LIST, em milissegundos. */
    private static final int TIMEOUT_LISTA_MS = 2000;

    /** Intervalo de espera da thread de escuta, em milissegundos. */
    private static final int INTERVALO_ESCUTA_MS = 1000;

    /** Identificador único do cliente, enviado no REGISTER e nas mensagens seINTERFACEntes. */
    private final String ID_CLIENTE;

    /** Endereço do servidor UDP. */
    private final String ENDERECO_DO_SERVIDOR;

    /** Porta do servidor UDP. */
    private final int PORTA_DO_SERVIDOR;

    /** Referência para a interface gráfica, usada apenas por callbacks na EDT. */
    private final InterfaceGrafica INTERFACE;

    /** Camada de transporte confiável sobre UDP. */
    private ProtocoloUDP PROTOCOLO;

    /** Indica se o cliente ainda está ativo e conectado ao fluxo do servidor. */
    private final AtomicBoolean ATIVO = new AtomicBoolean(false);

    /** Thread responsável por escutar UPDATE_LIST. */
    private Thread threadEscuta;

    /** Controle de sincronização entre o envio do LIST e a chegada da lista. */
    private final Object LISTA_DE_BLOQUEADOS = new Object();

    /** Marca se a lista foi recebida após o envio do LIST. */
    private boolean listaRecebida;



    /**
     * Constrói o cliente com o endereço do servidor e a interface gráfica.
     *
     * @param endereco endereço IP ou hostname do servidor
     * @param porta porta UDP do servidor
     * @param interfaceGrafica interface gráfica que apresentará mensagens e a lista de arquivos
     */
    public Cliente(String endereco, int porta, InterfaceGrafica interfaceGrafica) {
        
        ENDERECO_DO_SERVIDOR = endereco;
        
        PORTA_DO_SERVIDOR = porta;
        
        INTERFACE = interfaceGrafica;
        
        ID_CLIENTE = "cliente-" + UUID.randomUUID();// um trem que eu vi no curso de Spring Security da Alura
    }



    /**
     * Inicia a conexão UDP, ATIVO o cliente e dispara o registro em segundo plano.
     * Este método é chamado pela thread de eventos da interface ou pelo main,
     * mas nunca bloqueia a EDT.
     */
    public void iniciar () {

        try {
            PROTOCOLO = new ProtocoloUDP(ENDERECO_DO_SERVIDOR, PORTA_DO_SERVIDOR);

            ATIVO.set(true);

            registrarEmSegundoPlano();
        }

        catch (Exception e) {

            notificarServidorIndisponivel("Falha ao abrir a comunicação UDP: " + e.getMessage());
        }
    }



    /**
     * Retorna o identificador único do cliente.
     *
     * @return id do cliente no formato "cliente-UUID"
     */
    public String getIdCliente () { return ID_CLIENTE; }



    /**
     * Envia o REGISTER em uma thread própria, sem travar a interface.
     * Aguarda ACK dentro do timeout; se não houver resposta, avisa a interface
     * que o servidor está indisponível. Em caso de sucesso, inicia a escuta e o LIST.
     */
    private void registrarEmSegundoPlano () {

        Thread threadRegistro = new Thread(() -> {

            try {
                Map<String, String> campos = new HashMap<>();

                campos.put("id", ID_CLIENTE);

                Datagrama registro = Datagrama.registrar(ID_CLIENTE);

                PROTOCOLO.enviar(registro, new InetSocketAddress(PORTA_DO_SERVIDOR));

                Datagrama resposta = PROTOCOLO.receber(TIMEOUT_REGISTRO_MS).datagrama();

                if (resposta != null && resposta.getComando() == Comando.ACK) {

                    notificarStatus("Registro confirmado pelo servidor.");

                    iniciarEscutaEListar();
                }

                else if (resposta != null && resposta.getComando() == Comando.ERROR) {

                    String mensagem = resposta.getMensagemErro();

                    notificarServidorIndisponivel(
                        "Servidor recusou o registro: "
                        + (mensagem == null ? "erro desconhecido" : mensagem)
                    );
                }

                else notificarServidorIndisponivel("Não houve resposta do servidor ao REGISTER (timeout).");

            }

            catch (Exception e) {

                notificarServidorIndisponivel("Falha de comunicação com o servidor: " + e.getMessage());
            }
        }, "cliente-registro");

        threadRegistro.setDaemon(true);

        threadRegistro.start();
    }



    /**
     * Inicia a thread de escuta de UPDATE_LIST e solicita a lista inicial ao servidor.
     */
    private void iniciarEscutaEListar () {

        threadEscuta = new Thread(this::escutarAtualizacoes, "cliente-escuta-update-list");

        threadEscuta.setDaemon(true);

        threadEscuta.start();

        listarComTimeout();
    }



    /**
     * Loop da thread separada que recebe datagramas do servidor.
     * Apenas interpreta UPDATE_LIST e ERRO; a lista é entregue à interface
     * exclusivamente por SwingUtilities.invokeLater.
     */
    private void escutarAtualizacoes() {

        while (ATIVO.get()) {

            try {
                Datagrama datagrama = PROTOCOLO.receber(INTERVALO_ESCUTA_MS).datagrama();

                if (datagrama == null) continue;

                if (datagrama.getComando() == Comando.UPDATE_LIST) {

                    List<String> lista = extrairLista(datagrama);

                    SwingUtilities.invokeLater(() -> INTERFACE.atualizarLista(lista));

                    sinalizarListaRecebida();
                }

                else if ( (datagrama.getComando() == Comando.ACK) && (datagrama.getCampo("lista") != null) ) {

                    // Alguns servidores respondem ao LIST com ACK contendo a lista.

                    List<String> lista = extrairLista(datagrama);

                    SwingUtilities.invokeLater(() -> INTERFACE.atualizarLista(lista));

                    sinalizarListaRecebida();
                }

                else if (datagrama.getComando() == Comando.ERROR) {

                    String mensagem = datagrama.getMensagemErro();

                    SwingUtilities.invokeLater(() -> INTERFACE.exibirMensagem("Erro: " + (mensagem == null ? "mensagem desconhecida" : mensagem)));
                }
            }

            catch (Exception e) {

                if (ATIVO.get()) notificarServidorIndisponivel("Falha na escuta do servidor: " + e.getMessage());

                break;
            }
        }
    }



    /**
     * Envia o comando LIST em segundo plano e aguarda o recebimento da lista
     * dentro do timeout. Se a lista não chegar, considera o servidor indisponível.
     */
    private void listarComTimeout() {

        Thread threadListar = new Thread(() -> {

            try {
                Map<String, String> campos = new HashMap<>();

                campos.put("id", ID_CLIENTE);

                Datagrama pedidoLista = Datagrama.listar();

                PROTOCOLO.enviar(pedidoLista, new InetSocketAddress(PORTA_DO_SERVIDOR));

                synchronized (LISTA_DE_BLOQUEADOS) { listaRecebida = false; }

                synchronized (LISTA_DE_BLOQUEADOS) { LISTA_DE_BLOQUEADOS.wait(TIMEOUT_LISTA_MS); }

                if (!listaRecebida) notificarServidorIndisponivel("Não houve resposta do servidor ao LIST (timeout).");

            }

            catch (InterruptedException e) {

                Thread.currentThread().interrupt();
            }

            catch (Exception e) {

                notificarServidorIndisponivel("Falha ao solicitar a lista de arquivos: " + e.getMessage());
            }
        }, "cliente-listar");

        threadListar.setDaemon(true);

        threadListar.start();
    }



    /**
     * Marca o recebimento da lista e acorda a thread que aguarda o LIST.
     */
    private void sinalizarListaRecebida () {

        synchronized (LISTA_DE_BLOQUEADOS) {

            listaRecebida = true;

            LISTA_DE_BLOQUEADOS.notifyAll();
        }
    }



    /**
     * Extrai a lista de arquivos dos campos do datagrama.
     * Aceita separadores por quebra de linha ou ponto-e-vírgula.
     *
     * @param datagrama datagrama recebido do servidor
     * @return lista imutável de nomes de arquivos
     */
    private List<String> extrairLista (Datagrama datagrama) {

        String bruto = datagrama.getListaArquivos();

        if (bruto == null || bruto.isBlank()) return Collections.emptyList();

        return Arrays.asList(bruto.split("\\r?\\n|;"));
    }



    /**
     * Notifica a interface sobre servidor indisponível e encerra a conexão de forma limpa.
     *
     * @param mensagem motivo da indisponibilidade
     */
    private void notificarServidorIndisponivel (String mensagem) {

        if (!ATIVO.compareAndSet(true, false)) return;

        if (PROTOCOLO != null) {

            try {
                PROTOCOLO.fechar();
            }

            catch (Exception e) {

                // A intenção é apenas encerrar; eventuais erros de fechamento são ignorados.
            }
        }

        if (threadEscuta != null) threadEscuta.interrupt();

        SwingUtilities.invokeLater(() -> INTERFACE.exibirServidorIndisponivel(mensagem));
    }



    /**
     * Exibe uma mensagem comum na interface, sempre na EDT.
     *
     * @param mensagem texto informativo
     */
    private void notificarStatus (String mensagem) {

        SwingUtilities.invokeLater(() -> INTERFACE.exibirMensagem(mensagem));
    }



    /**
     * Encerra o cliente, interrompe a escuta e fecha o PROTOCOLO UDP.
     */
    public void encerrar() {

        ATIVO.set(false);

        if (threadEscuta != null) threadEscuta.interrupt();

        if (PROTOCOLO != null) {

            try {
                PROTOCOLO.fechar();
            }

            catch (Exception e) {
                // Ignora erros de encerramento.
            }
        }
    }



    /**
     * Ponto de entrada da aplicação.
     * Uso:
     * java cliente.Cliente [servidor] [porta]
     *
     * @param args argumentos opcionais de linha de comando
     */
    public static void main (String[] args) {

        final String servidor = args.length > 0? args[0] : "localhost";

        final int porta = args.length > 1? Integer.parseInt(args[1]) : 5000;

        SwingUtilities.invokeLater(() -> {

            InterfaceGrafica janela = new InterfaceGrafica();

            Cliente cliente = new Cliente(servidor, porta, janela);

            cliente.iniciar();
        });
    }
}