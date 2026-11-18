/*
 * Projeto: Servidor de Arquivos via UDP
 * Classe: LogServidor
 * Pacote: src/servidor
 *
 * Responsabilidade:
 *   Registrar as operações do servidor em log_servidor.txt no formato:
 *   [AAAA-MM-DD HH:MM:SS] [IP:porta] [OPERAÇÃO] [STATUS] detalhes
 *
 * Características:
 *   - Escrita em modo de acréscimo (append);
 *   - Descarga imediata do conteúdo a cada registro;
 *   - Acesso sincronizado entre as threads do servidor;
 *   - Criação automática do arquivo e de diretórios, se necessário;
 *   - Registro no fuso horário local do servidor.
 *
 * Operações registradas, conforme a instrução do projeto:
 *   - CONEXAO: abertura de conexão/registro de cliente;
 *   - UPLOAD: recebimento de arquivo;
 *   - DOWNLOAD: envio de arquivo;
 *   - RETRANSMISSAO: retransmissão de fragmento;
 *   - TIMEOUT: estouro de tempo de espera;
 *   - ERRO: erros em geral.
 *
 * Observação: esta classe usa apenas recursos do JDK (java.io e java.time).
 */

package servidor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogServidor {

    /** Nome padrão do arquivo de log, conforme definido no projeto. */
    private static final String NOME_ARQUIVO_LOG = "log_servidor.txt";

    /** Formato da data/hora exigido pelo projeto: AAAA-MM-DD HH:MM:SS. */
    private static final DateTimeFormatter FORMATO_DATA_HORA =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Arquivo de log usado pela instância. */
    private final File arquivoLog;

    /**
     * Objeto usado como trava para sincronizar o acesso ao arquivo entre
     * as threads do servidor. Garante que duas gravações não se misturem.
     */
    private final Object travaEscrita = new Object();

    /**
     * Constrói o LogServidor e garante que o arquivo de log exista.
     * O arquivo é criado no diretório de trabalho atual do servidor.
     */
    public LogServidor() {
        this.arquivoLog = new File(NOME_ARQUIVO_LOG);
        garantirExistenciaDoArquivo();
    }

    /**
     * Constrói o LogServidor apontando para um caminho específico.
     *
     * @param diretorioLog diretório onde o arquivo de log será criado;
     *                     se for null, usa o diretório atual.
     */
    public LogServidor(File diretorioLog) {
        File diretorio = (diretorioLog == null) ? new File(".") : diretorioLog;
        this.arquivoLog = new File(diretorio, NOME_ARQUIVO_LOG);
        garantirExistenciaDoArquivo();
    }

    /**
     * Registra a abertura de conexão/registro de um cliente.
     *
     * @param enderecoCliente IP:porta do cliente, sem colchetes;
     * @param detalhes        informação complementar da operação.
     */
    public void registrarConexao(String enderecoCliente, String detalhes) {
        registrar(enderecoCliente, "CONEXAO", "OK", detalhes);
    }

    /**
     * Registra uma operação de subir de arquivo.
     *
     * @param enderecoCliente IP:porta do cliente, sem colchetes;
     * @param status          status da operação (ex.: OK, ERRO);
     * @param detalhes        informação complementar (nome do arquivo, etc.).
     */
    public void registrarUpload(String enderecoCliente, String status, String detalhes) {
        registrar(enderecoCliente, "UPLOAD", status, detalhes);
    }

    /**
     * Registra uma operação de baixar de arquivo.
     *
     * @param enderecoCliente IP:porta do cliente, sem colchetes;
     * @param status          status da operação (ex.: OK, ERRO);
     * @param detalhes        informação complementar (nome do arquivo, etc.).
     */
    public void registrarDownload(String enderecoCliente, String status, String detalhes) {
        registrar(enderecoCliente, "DOWNLOAD", status, detalhes);
    }

    /**
     * Registra uma retransmissão de fragmento.
     *
     * @param enderecoCliente IP:porta do cliente, sem colchetes;
     * @param numeroFragmento número do fragmento retransmitido;
     * @param detalhes        informação complementar.
     */
    public void registrarRetransmissao(String enderecoCliente, int numeroFragmento, String detalhes) {
        String detalheCompleto = "fragmento " + numeroFragmento
                + (detalhes == null || detalhes.isBlank() ? "" : " - " + detalhes);
        registrar(enderecoCliente, "RETRANSMISSAO", "OK", detalheCompleto);
    }

    /**
     * Registra um esgotar.
     *
     * @param enderecoCliente IP:porta do cliente, sem colchetes;
     * @param detalhes        informação complementar sobre o esgotar.
     */
    public void registrarTimeout(String enderecoCliente, String detalhes) {
        registrar(enderecoCliente, "TIMEOUT", "ERRO", detalhes);
    }

    /**
     * Registra um erro genérico do servidor.
     *
     * @param enderecoCliente IP:porta do cliente (ou "SERVIDOR" quando não
     *                        houver cliente associado), sem colchetes;
     * @param detalhes        informação complementar sobre o erro.
     */
    public void registrarErro(String enderecoCliente, String detalhes) {
        registrar(enderecoCliente, "ERRO", "ERRO", detalhes);
    }

    /**
     * Monta a linha de log no formato definido pelo projeto e persiste no
     * arquivo:
     *
     * [AAAA-MM-DD HH:MM:SS] [IP:porta] [OPERAÇÃO] [STATUS] detalhes
     *
     * @param enderecoCliente endereço no formato IP:porta, sem colchetes;
     * @param operacao        nome da operação em maiúsculas;
     * @param status          status da operação em maiúsculas;
     * @param detalhes        detalhe livre da operação.
     */
    void registrar(String enderecoCliente, String operacao,
                   String status, String detalhes) {
        String linha = montarLinha(enderecoCliente, operacao, status, detalhes);

        // A gravação é sincronizada para evitar que duas threads do servidor
        // escrevam no mesmo arquivo ao mesmo tempo e misturem as linhas.
        synchronized (travaEscrita) {
            try (PrintWriter escritor = new PrintWriter(new BufferedWriter(
                    new FileWriter(arquivoLog, StandardCharsets.UTF_8, true)))) {
                escritor.println(linha);
                // PrintWriter faz flush implícito a cada println, mas o flush
                // explícito garante a descarga imediata no disco.
                escritor.flush();
            } catch (IOException excecao) {
                // Falha ao gravar o log não pode derrubar o servidor.
                // A mensagem é exibida no console para diagnóstico.
                System.err.println("[LogServidor] Falha ao gravar no arquivo de log: "
                        + excecao.getMessage());
            }
        }
    }

    /**
     * Monta a linha no formato exato da instrução do projeto:
     * [data] [ip:porta] [operação] [status] detalhe
     */
    private String montarLinha(String enderecoCliente, String operacao,
                               String status, String detalhes) {
        String dataHora = LocalDateTime.now().format(FORMATO_DATA_HORA);
        String endereco = (enderecoCliente == null || enderecoCliente.isBlank())
                ? "-"
                : enderecoCliente;
        String detalhe = (detalhes == null || detalhes.isBlank()) ? "" : detalhes;

        return "[" + dataHora + "] "
                + "[" + endereco + "] "
                + "[" + operacao + "] "
                + "[" + status + "]"
                + (detalhe.isEmpty() ? "" : " " + detalhe);
    }

    /**
     * Garante que o arquivo de log exista, criando diretórios e o próprio
     * arquivo vazio quando necessário.
     */
    private void garantirExistenciaDoArquivo() {
        try {
            File diretorioPai = arquivoLog.getAbsoluteFile().getParentFile();
            if (diretorioPai != null && !diretorioPai.exists()) {
                diretorioPai.mkdirs();
            }

            if (!arquivoLog.exists()) {
                arquivoLog.createNewFile();
            }
        } catch (IOException excecao) {
            System.err.println("[LogServidor] Falha ao criar o arquivo de log: "
                    + excecao.getMessage());
        }
    }
}
