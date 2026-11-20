package cliente.interfaceGrafica;

/**
 * Canal de comunicação com a interface. Todos os métodos são chamados
 * fora da EDT; cabe à GUI agendar a atualização visual via
 * SwingUtilities.invokeLater.
 */
public interface Monitor {

    /**
     * Fração concluída no intervalo [0, 1].
     */
    void onProgresso(double fracao);

    /**
     * Mensagem de status (ex.: "Enviando fragmento 3 de 12").
     */
    void onStatus(String mensagem);

    /**
     * Transferência concluída com sucesso.
     */
    void onConcluido(String mensagem);

    /**
     * Transferência abortada por erro.
     */
    void onErro(String mensagem);
}