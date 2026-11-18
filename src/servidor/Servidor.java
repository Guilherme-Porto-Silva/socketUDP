package servidor;

import comum.Comando;
import comum.Datagrama;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Servidor central do sistema de arquivos via UDP.
 *
 * Concorrência: uma thread por cliente, criada quando o comando REGISTER é
 * recebido. A responsabilidade de transferência fica na ClienteHandler e o
 * Servidor se mantém livre para aceitar novos registros.
 *
 * Notificação em tempo real: a cada alteração na lista de arquivos o servidor
 * envia UPDATE_LIST para todos os clientes ativos.
 */
public class Servidor {

    /**
     * Porta padrão do servidor.
     *
     * Escolha: 5000 é uma porta alta, fora do range privilegiado (1-1024),
     * não conflita com serviços comuns do sistema e é fácil de ler/digitar em
     * testes de laboratório. O valor pode ser sobrescrito por argumento da
     * linha de comando.
     */
    public static final int PORTA = 5000;

    private static final int TAMANHO_BUFFER = 8192;

    private final DatagramSocket socket;
    private final GerenciadorArquivos gerenciadorArquivos;
    private final LogServidor log;

    /**
     * Relação de clientes ativos, indexada pelo endereço IP:porta do cliente.
     * ConcurrentHashMap garante visibilidade e operações atômicas durante os
     * acessos concorrentes feitos pelas ClienteHandler.
     */
    private final Map<InetSocketAddress, ClienteHandler> clientesAtivos =
            new ConcurrentHashMap<>();

    private volatile boolean executando = true;

    public Servidor() throws SocketException {
        this(PORTA);
    }

    public Servidor(int porta) throws SocketException {
        this.socket = new DatagramSocket(porta);
        this.gerenciadorArquivos = new GerenciadorArquivos();
        this.log = new LogServidor();
    }

    /**
     * Ponto de entrada do servidor. Permite opcionalmente passar a porta como
     * primeiro argumento; caso contrário, usa a constante PORTA.
     */
    public static void main(String[] args) {
        int porta = PORTA;
        if (args.length > 0) {
            try {
                porta = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                // Mantém a porta padrão quando o argumento não é numérico.
                porta = PORTA;
            }
        }

        Servidor servidor;
        try {
            servidor = new Servidor(porta);
        } catch (SocketException e) {
            System.err.println("[Servidor] Nao foi possivel abrir a porta " + porta + ": " + e.getMessage());
            System.exit(1);
            return;
        }
        servidor.iniciar();
    }

    /**
     * Abre o loop principal: aguarda REGISTER e cria uma ClienteHandler por
     * cliente. O loop nunca processa uma transferência por conta própria, o
     * que garante que o servidor continue aceitando novos clientes.
     */
    public void iniciar() {
        log.registrar("SERVIDOR", "INICIO", "Aguardando registros na porta " + socket.getLocalPort());
        System.out.println("[Servidor] Aguardando registros na porta " + socket.getLocalPort());

        while (executando) {
            byte[] buffer = new byte[TAMANHO_BUFFER];
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pacote);
            } catch (IOException e) {
                if (!executando) {
                    break;
                }
                log.registrar("SERVIDOR", "ERRO", "Falha ao receber pacote: " + e.getMessage());
                continue;
            }
            tratarPacoteRecebido(pacote);
        }

        socket.close();
        log.registrar("SERVIDOR", "ENCERRAMENTO", "Servidor encerrado.");
    }

    private void tratarPacoteRecebido(DatagramPacket pacote) {
        InetSocketAddress remetente = (InetSocketAddress) pacote.getSocketAddress();
        Datagrama datagrama;
        try {
            byte[] dados = Arrays.copyOf(pacote.getData(), pacote.getLength());
            datagrama = Datagrama.deserializar(dados);
        } catch (Exception e) {
            log.registrar("SERVIDOR", "ERRO", "Datagrama invalido de " + remetente + ": " + e.getMessage());
            return;
        }

        // Apenas REGISTER é tratado no loop principal. Pacotes de clientes já
        // registrados são processados pela respectiva ClienteHandler, que roda
        // em thread dedicada.
        if (datagrama.comando != Comando.REGISTER) {
            if (!clientesAtivos.containsKey(remetente)) {
                enviarErro(remetente, datagrama, "Cliente nao registrado. Envie REGISTER primeiro.");
            }
            return;
        }

        if (clientesAtivos.containsKey(remetente)) {
            // Cliente já está registrado; apenas reenvia o ACK para manter a
            // consistência com o protocolo e evita threads duplicadas.
            enviarAck(remetente, datagrama.idCliente);
            log.registrar("REGISTER", "DUPLICADO", "Cliente " + remetente + " ja registrado.");
            return;
        }

        ClienteHandler handler = new ClienteHandler(socket, remetente, gerenciadorArquivos, log, this);
        clientesAtivos.put(remetente, handler);
        Thread threadHandler = new Thread(handler, "ClienteHandler-" + remetente);
        threadHandler.setDaemon(true);
        threadHandler.start();

        enviarAck(remetente, datagrama.idCliente);
        log.registrar("REGISTER", "OK", "Cliente registrado: " + remetente + " (id=" + datagrama.idCliente + ")");
    }

    private void enviarAck(InetSocketAddress destino, String idCliente) {
        Datagrama ack = new Datagrama();
        ack.comando = Comando.ACK;
        ack.idCliente = idCliente;
        ack.mensagem = "REGISTER aceito";
        enviar(destino, ack, "ACK");
    }

    private void enviarErro(InetSocketAddress destino, Datagrama original, String mensagem) {
        Datagrama erro = new Datagrama();
        erro.comando = Comando.ERROR;
        erro.idCliente = original.idCliente;
        erro.codigoErro = 1;
        erro.mensagem = mensagem;
        enviar(destino, erro, "ERROR");
    }

    private void enviar(InetSocketAddress destino, Datagrama datagrama, String operacao) {
        try {
            byte[] dados = Datagrama.serialize(datagrama);
            socket.send(new DatagramPacket(dados, dados.length, destino));
        } catch (Exception e) {
            log.registrar(operacao, "ERRO", "Falha ao enviar para " + destino + ": " + e.getMessage());
        }
    }

    /**
     * Remove o cliente da relação de ativos. Chamado pela ClienteHandler no
     * encerramento da transferência (sucesso ou falha).
     */
    public void removerCliente(InetSocketAddress endereco) {
        ClienteHandler removido = clientesAtivos.remove(endereco);
        if (removido != null) {
            log.registrar("CLIENTE", "REMOCAO", "Cliente removido: " + endereco);
        }
    }

    /**
     * Envia UPDATE_LIST para todos os clientes ativos, refletindo a lista de
     * arquivos atual do servidor.
     */
    public void notificarAtualizacaoLista() {
        List<String> lista = gerenciadorArquivos.listarArquivos();
        Datagrama atualizacao = new Datagrama();
        atualizacao.comando = Comando.UPDATE_LIST;
        atualizacao.listaArquivos = lista == null ? new ArrayList<>() : lista;
        atualizacao.mensagem = "Lista de arquivos atualizada";

        try {
            byte[] dados = Datagrama.serializar(atualizacao);

            // Itera sobre um snapshot da coleção. Clientes encerrados já foram
            // removidos; falhas individuais de envio são registradas como aviso
            // e não derrubam o laço de broadcast.
            for (InetSocketAddress cliente : new ArrayList<>(clientesAtivos.keySet())) {
                try {
                    socket.send(new DatagramPacket(dados, dados.length, cliente));
                } catch (Exception e) {
                    log.registrar("UPDATE_LIST", "AVISO", "Falha ao enviar lista para " + cliente + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            log.registrar("UPDATE_LIST", "ERRO", "Falha ao serializar lista: " + e.getMessage());
        }

        log.registrar("UPDATE_LIST", "OK", "Lista enviada para " + clientesAtivos.size() + " cliente(s).");
    }

    /**
     * Encerra o loop principal e fecha o socket. Pode ser chamado por outra
     * thread para parada controlada do servidor.
     */
    public void encerrar() {
        executando = false;
        socket.close();
    }

    public DatagramSocket getSocket() {
        return socket;
    }

    public GerenciadorArquivos getGerenciadorArquivos() {
        return gerenciadorArquivos;
    }

    public LogServidor getLog() {
        return log;
    }
}
